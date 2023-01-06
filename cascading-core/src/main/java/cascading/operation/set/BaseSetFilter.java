package cascading.operation.set;

public class BaseSetFilter extends BaseOperation<BaseSetFilter.Context> implements Filter<BaseSetFilter.Context>{
    public static final int EXPECTED_INSERTIONS = 10_000_000;
    protected final int expectedInsertions;
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    public enum StoredKeyCounter {
        Stored,
        Contains
    }

    public enum MatchedKeyCounter {
        Tested,
        Contains
    }

    public enum KeyDuration {
        Test_Duration_Ns,
        Put_Duration_Ns
    }

    protected static class Context {
        public Set<Long> filter;
        public long putCount;
        public long testCount;
        public long containsCount;
    }

    public BaseSetFilter() {
        this.expectedInsertions = EXPECTED_INSERTIONS;
    }

    public BaseSetFilter(int expectedInsertions) {
        this.expectedInsertions = expectedInsertions;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
        Context context = new Context();

        context.filter = new LongOpenHashSet(expectedInsertions);

        operationCall.setContext(context);
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall<Context> filterCall) {
        TupleEntry arguments = filterCall.getArguments();
        Context context = filterCall.getContext();

        long value = arguments.getLong(0);

        boolean contains = testFilter(flowProcess, context, value);

        context.testCount++;

        if (contains) {
            context.containsCount++;
        }

        return contains;
    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall<Context> operationCall) {
        Context context = operationCall.getContext();

        if (context == null) {
            return;
        }

        String arguments = operationCall.getArgumentFields().print();

        if (context.testCount != 0 && context.containsCount == 0) {
            LOG.warn("filter on: {}, with insertions: {}, tested: {}, found no matches", arguments, context.filter.size(), context.testCount);
        } else {
            LOG.info("filter on: {}, with insertions: {}, tested: {}, found: {}", arguments, context.filter.size(), context.testCount, context.containsCount);
        }

        LogUtil.logMemory(LOG, "final memory with open hashset on: " + arguments);
    }

    protected abstract boolean testFilter(FlowProcess flowProcess, Context context, long key);

    protected boolean putLong(FlowProcess flowProcess, Context context, long value) {
        long start = System.nanoTime();

        boolean put = context.filter.add(value);

        flowProcess.increment(KeyDuration.Put_Duration_Ns, System.nanoTime() - start);

        if (put) {
            flowProcess.increment(StoredKeyCounter.Stored, 1);
        } else {
            flowProcess.increment(StoredKeyCounter.Contains, 1);
        }

        context.putCount++;

        return put;
    }
    
}
