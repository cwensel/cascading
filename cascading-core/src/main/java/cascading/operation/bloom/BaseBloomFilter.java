package cascading.operation.bloom;

public class BaseBloomFilter  extends BaseOperation<BaseBloomFilter.Context> implements Filter<BaseBloomFilter.Context>{
    public static final double FALSE_POSITIVE_PROB = 0.000001;  // 20 hashes
    public static final long EXPECTED_INSERTIONS = 10_000_000L;
    protected final ScalableBloomFilter.Rate growthRate;
    protected final float errorProbabilityRatio;
    protected final long expectedInsertions;
    protected final double fpp;
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
        public ScalableBloomFilter filter;
        public TupleBytesOutputStream bytes;
        public long putCount;
        public long testCount;
        public long containsCount;
    }

    public BaseBloomFilter(long expectedInsertions, double fpp) {
        this.growthRate = ScalableBloomFilter.Rate.SLOW;
        this.errorProbabilityRatio = ScalableBloomFilter.ERROR_PROBABILITY_RATIO;
        this.expectedInsertions = expectedInsertions;
        this.fpp = fpp;
    }

    public BaseBloomFilter() {
        this.growthRate = ScalableBloomFilter.Rate.SLOW;
        this.errorProbabilityRatio = ScalableBloomFilter.ERROR_PROBABILITY_RATIO;
        this.expectedInsertions = EXPECTED_INSERTIONS;
        this.fpp = FALSE_POSITIVE_PROB;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
        Fields argumentFields = operationCall.getArgumentFields();
        Context context = new Context();

        context.filter = new ScalableBloomFilter(argumentFields.print(), growthRate, errorProbabilityRatio, expectedInsertions, fpp);
        Type[] types = argumentFields.getTypes();
        Class[] canonicalTypes = null;

        if (types == null) {
            LOG.info("no argument types declared for: {}", argumentFields);
        } else {
            canonicalTypes = Coercions.getCanonicalTypes(types);
        }

        context.bytes = new TupleBytesOutputStream(canonicalTypes);

        operationCall.setContext(context);
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall<Context> filterCall) {
        TupleEntry arguments = filterCall.getArguments();
        Context context = filterCall.getContext();

        ByteBuffer bytes = writeReturnBytes(context.bytes, arguments.getTuple());

        // if we have likely seen it before, we should remove it
        boolean contains = testFilter(flowProcess, context, bytes);

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
            LOG.warn("filter on: {}, with insertions: {}, tested: {}, found no matches", arguments, context.filter.getInsertCount(), context.testCount);
        } else {
            LOG.info("filter on: {}, with insertions: {}, tested: {}, found: {}", arguments, context.filter.getInsertCount(), context.testCount, context.containsCount);
        }

        LogUtil.logMemory(LOG, "final memory with bloom on: " + arguments);
    }

    protected abstract boolean testFilter(FlowProcess flowProcess, Context context, ByteBuffer bytes);

    protected boolean putBytes(FlowProcess flowProcess, Context context, ByteBuffer bytes) {
        long start = System.nanoTime();

        boolean put = context.filter.put(bytes);

        flowProcess.increment(KeyDuration.Put_Duration_Ns, System.nanoTime() - start);

        if (put) {
            flowProcess.increment(StoredKeyCounter.Stored, 1);
        } else {
            flowProcess.increment(StoredKeyCounter.Contains, 1);
        }

        context.putCount++;

        return put;
    }

    protected ByteBuffer writeReturnBytes(TupleBytesOutputStream bytes, Tuple tuple) {
        try {
            bytes.reset();
            bytes.writeTuple(tuple);
            bytes.flush();

            return bytes.toByteBuffer();
        } catch (IOException exception) {
            throw new CascadingException("unable to write tuple bytes", exception);
        }
    }
    
}
