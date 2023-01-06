package cascading.operation.set;

public class AccumulatingSetFilter extends BaseSetFilter {
    public AccumulatingSetFilter(int expectedInsertions) {
        super(expectedInsertions);
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
        super.prepare(flowProcess, operationCall);

        Fields arguments = operationCall.getArgumentFields();

        if (arguments.size() > 1) {
            throw new IllegalArgumentException("may only have one argument that will coerce into a long, got: " + arguments.print());
        }
    }

    @Override
    protected boolean testFilter(FlowProcess flowProcess, Context context, long value) {
        flowProcess.increment(MatchedKeyCounter.Tested, 1);
        return !putLong(flowProcess,context,value);
    }
    
}
