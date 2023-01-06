package cascading.operation.bloom;

public class AccumulatingBloomFilter  extends BaseBloomFilter {
    public AccumulatingBloomFilter(long expectedInsertions, double fpp) {
        super(expectedInsertions, fpp);
    }

    @Override
    protected boolean testFilter(FlowProcess flowProcess, Context context, ByteBuffer bytes) {
        return putBytes(flowProcess, context, bytes);
    }
    
}
