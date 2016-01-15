package cascading.flow.local.stream.duct;

import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Fork;
import cascading.tuple.TupleEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * This "Fork" avoids a possible deadlock in Fork-and-Join scenarios by running downstream edges into parallel threads.
 *
 * Forked from cascading.flow.stream.duct.Fork by Cyrille Chepelov <cch@transparencyrights.com> on 2016-01-07.
 */
public class ParallelFork<Outgoing> extends Fork<TupleEntry, Outgoing> {

    private static final Logger LOG = LoggerFactory.getLogger( ParallelFork.class );

    abstract static class Message {
        final protected Duct previous;

        public Message(Duct previous) {
            this.previous = previous;
        }

        abstract public void passOn(Duct next);
        abstract public boolean isTermination();
    }

    static final class StartMessage extends Message {
        final CountDownLatch startLatch;

        public StartMessage(Duct previous, CountDownLatch startLatch) {
            super(previous);
            this.startLatch = startLatch;
        }
        public void passOn(Duct next) {
            startLatch.countDown();
            next.start(previous);
        }
        public boolean isTermination() { return false; }
    }

    static final class ReceiveMessage extends Message {
        final TupleEntry tuple;

        public ReceiveMessage(Duct previous, TupleEntry tuple) {
            super(previous);
            this.tuple = new TupleEntry(tuple); /* we make a new copy right here, to avoid cross-thread trouble when upstream changes the tuple */
        }
        public void passOn(Duct next) {
            next.receive(previous, tuple);
        }
        public boolean isTermination() { return false; }
    }

    static final class CompleteMessage extends Message {
        public CompleteMessage(Duct previous) {
            super(previous);
        }
        public void passOn(Duct next) {
            next.complete(previous);
        }
        public boolean isTermination() { return true; }
    }

    private final ArrayList<LinkedBlockingQueue<Message>> buffers;
    private final ExecutorService executor;
    private final ArrayList<Callable<Throwable>> actions;
    private final ArrayList<Future<Throwable>> futures;

    public ParallelFork(Duct[] allNext) {
        super(allNext);

        this.executor = Executors.newFixedThreadPool(allNext.length);

        /* Obvious choices for nThread in newFixedThreadPool:
            * nThreads = allNext.length. Potential to create a lot of thread-thrashing on machines with few cores, but
                        the OS scheduler should ensure any executable thread gets a chance to proceed (and possibly
                        complete, enabling others down a Local*Gate to proceed)
            * nThreads = #of CPU. Would work, possibly by chance as long as #of CPU is "big enough" (see below)
            * nThreads=1 : "parallel" is parallel with respect to upstream. This could work sometimes, but will still
                deadlock in the Fork-CoGroup-HashJoin scenario, as the other side of join could still be starved by
                one side not completing.
            * nThreads = max(# of pipes merged into a CoGroup or HashJoin downstream from here). This is the minimum
                required to guarantee one side can't starve another. It COULD probably be queried from the flow graph,
                factoring in for all potential combinations...

            Therefore, the easy safe choice is to take allNext.length.
        */

        ArrayList<LinkedBlockingQueue<Message>> buffers = new ArrayList<LinkedBlockingQueue<Message>>(allNext.length);
        ArrayList<Future<Throwable>> futures = new ArrayList<Future<Throwable>>(allNext.length);
        ArrayList<Callable<Throwable>> actions = new ArrayList<Callable<Throwable>>(allNext.length);

        final ParallelFork self = this;
        for (int i = 0; i < allNext.length; ++i) {
            final LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<Message>();
            final Duct next = allNext[i];

            buffers.add(queue);
            Callable<Throwable> action = new Callable<Throwable>() {
                @Override
                public Throwable call() throws Exception {
                    try {
                        while (true) {
                            Message message = queue.take();
                            message.passOn(next);
                            if (message.isTermination()) {
                                return null;
                            }
                        }
                    } catch (Throwable throwable) {
                        return throwable;
                    }
                }
            };

            actions.add(action);
        }
        this.buffers = buffers;
        this.actions = actions;
        this.futures = futures;
    }

    @Override
    public void initialize() {
        super.initialize();
    }

    private void broadcastMessage(Message message) {
        for (LinkedBlockingQueue<Message> queue: buffers) {
            queue.offer(message);
        }
    }

    private WeakReference<Duct> started = null;

    @Override
    public void start(Duct previous) {
        LOG.debug("StartMessage {} BEGIN", previous);
        synchronized (this) {
            if (started != null) {
                LOG.error("ParallelFork already started! former previous={}, new previous={}", started.get(), previous);
                return;
            }
            if (completed != null) {
                throw new IllegalStateException("cannot start an already completed ParallelFork");
            }
            started = new WeakReference<Duct>(previous);
        }

        try {
            for (Callable<Throwable> action : actions) {
                Future<Throwable> future = executor.submit(action);
                futures.add(future);
            }

            CountDownLatch startLatch = new CountDownLatch(allNext.length);
            broadcastMessage(new StartMessage(previous, startLatch));

            startLatch.await(); // wait for all threads to have started
        } catch (InterruptedException iex) {
            throw new UndeclaredThrowableException(iex);
        }
    }

    @Override
    public void receive(Duct previous, TupleEntry incoming) {
        broadcastMessage(new ReceiveMessage(previous, incoming ));
            /* incoming is copied once for each downstream pipe, within the current thread. */
    }

    private WeakReference<Duct> completed = null; /* records origin duct */


    @Override
    public void complete(Duct previous) {
        synchronized (this) {
            if (completed != null) {
                LOG.error("ParallelFork already complete! former previous={} new previous={}", completed.get(), previous);
                return;
            }
            completed = new WeakReference<Duct>(previous);
        }

        broadcastMessage(new CompleteMessage(previous));
        /* the CompleteMessage will cause the downstream threads to complete */

        try {
            for (Future<Throwable> future : futures) {
                Throwable throwable;
                try {
                    throwable = future.get();
                } catch (InterruptedException iex) {
                    throwable = iex;
                } catch (ExecutionException cex) {
                    throwable = cex;
                }

                if (throwable != null) {
                    throw new RuntimeException(throwable);
                }
            }
        } finally {
            executor.shutdown();
        }
    }
}
