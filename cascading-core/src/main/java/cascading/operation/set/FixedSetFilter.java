package cascading.operation.set;

public class FixedSetFilter extends BaseSetFilter{
    private static final Logger LOG = LoggerFactory.getLogger(FixedSetFilter.class);
    SerPredicate<TupleEntry> predicate = t -> true;
    private Tap<?, ?, ?> sourceTap;

    public FixedSetFilter(Tap<?, ?, ?> sourceTap) {
        this.sourceTap = sourceTap;

        if (sourceTap == null) {
            throw new IllegalArgumentException("sourceTap may not be null");
        }
    }

    public FixedSetFilter(Tap<?, ?, ?> sourceTap, SerPredicate<TupleEntry> predicate) {
        this.sourceTap = sourceTap;
        this.predicate = predicate;
    }

    public FixedSetFilter(Tap<?, ?, ?> sourceTap, int expectedInsertions, SerPredicate<TupleEntry> predicate) {
        super(expectedInsertions);
        this.sourceTap = sourceTap;
        this.predicate = predicate;
    }

    public FixedSetFilter(Tap<?, ?, ?> sourceTap, int expectedInsertions) {
        super(expectedInsertions);
        this.sourceTap = sourceTap;

        if (sourceTap == null) {
            throw new IllegalArgumentException("sourceTap may not be null");
        }
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
        super.prepare(flowProcess, operationCall);

        Context context = operationCall.getContext();
        Fields arguments = operationCall.getArgumentFields();

        if (arguments.size() > 1) {
            throw new IllegalArgumentException("may only have one argument that will coerce into a long, got: " + arguments.print());
        }

        LOG.info("started loading data on: {}, from: {}", arguments.print(), sourceTap);

        TupleEntryStream.entryStream(sourceTap, flowProcess)
                .filter(predicate) // consider just wrapping the Tap with one that filters the data
                .forEach(entry -> putLong(flowProcess, context, entry.getLong(arguments)));

        if (context.putCount == 0) {
            LOG.warn("on: {}, from: {}, read no tuples", arguments.print(), sourceTap);
            throw new TapException("no filter join data read from: " + sourceTap);
        }

        LOG.info("completed loading data on: {}, from: {}", arguments.print(), sourceTap);
        LOG.info("read tuples: {}, with uniques: {}, duplicates: {}", context.putCount, context.filter.size(), context.putCount - context.filter.size());
    }

    @Override
    protected boolean testFilter(FlowProcess flowProcess, Context context, long value) {
        flowProcess.increment(MatchedKeyCounter.Tested, 1);

        long start = System.nanoTime();

        boolean contains = context.filter.contains(value);

        flowProcess.increment(KeyDuration.Test_Duration_Ns, System.nanoTime() - start);

        if (contains) {
            flowProcess.increment(MatchedKeyCounter.Contains, 1);
        }

        return contains;
    }
    
}
