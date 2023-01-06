package cascading.operation.bloom;

public class FixedBloomFilter extends BaseBloomFilter{
    private static final Logger LOG = LoggerFactory.getLogger(FixedBloomFilter.class);
    SerPredicate<TupleEntry> predicate = t -> true;
    private Tap<?, ?, ?> sourceTap;

    public FixedBloomFilter(Tap<?, ?, ?> sourceTap, long expectedInsertions, double fpp, SerPredicate<TupleEntry> predicate) {
        super(expectedInsertions, fpp);
        this.sourceTap = sourceTap;
        this.predicate = predicate;

        if (sourceTap == null) {
            throw new IllegalArgumentException("sourceTap may not be null");
        }
    }

    public FixedBloomFilter(Tap<?, ?, ?> sourceTap, long expectedInsertions, double fpp) {
        super(expectedInsertions, fpp);
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
        Tuple result = new Tuple();

        LOG.info("started loading data on: {}, from: {}", arguments.print(), sourceTap);

        TupleEntryStream.entryStream(sourceTap, flowProcess)
                .filter(predicate) // consider just wrapping the Tap with one that filters the data
                .forEach(entry -> {

                    entry.selectInto(arguments, result);
                    ByteBuffer bytes = writeReturnBytes(context.bytes, result);

                    putBytes(flowProcess, context, bytes);

                    result.clear();

                });

        if (context.putCount == 0) {
            LOG.error("on: {}, from: {}, read no tuples", arguments.print(), sourceTap);
            throw new TapException("no filter join data read from: " + sourceTap);
        }

        LOG.info("completed loading data on: {}, from: {}", arguments.print(), sourceTap);
        LOG.info("read tuples: {}, with uniques: {}, duplicates: {}", context.putCount, context.filter.getInsertCount(), context.putCount - context.filter.getInsertCount());
    }

    @Override
    protected boolean testFilter(FlowProcess flowProcess, Context context, ByteBuffer bytes) {
        flowProcess.increment(MatchedKeyCounter.Tested, 1);

        long start = System.nanoTime();

        boolean mightContain = context.filter.mightContain(bytes);

        flowProcess.increment(KeyDuration.Test_Duration_Ns, System.nanoTime() - start);

        if (mightContain) {
            flowProcess.increment(MatchedKeyCounter.Contains, 1);
        }

        return mightContain;
    }
    
}
