package cascading;

import cascading.flow.Flow;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.pipe.*;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static data.InputData.inputFileLower;
import static data.InputData.inputFileUpper;

public class FailingSelfJoinTest extends PlatformTestCase {

    public FailingSelfJoinTest() {
        super(true); // leave cluster testing enabled
    }

    @Test
    public void testFailingJob() throws Exception {
        getPlatform().copyFromLocal(inputFileLower);
        getPlatform().copyFromLocal(inputFileUpper);

        Map sources = new HashMap();
        sources.put("lower", getPlatform().getTextFile(inputFileLower));
        sources.put("upper", getPlatform().getTextFile(inputFileUpper));

        Tap sink = getPlatform().getTextFile(getOutputPath("sink"), SinkMode.REPLACE);

        Pipe pipeLower = new Each(new Pipe("lower"), new Fields("line"), new Identity(new Fields("lineLower")));
        Pipe pipeUpper = new Each(new Pipe("upper"), new Fields("line"), new Identity(new Fields("line")));

        Pipe upperGroupBy = new GroupBy(new Pipe[]{pipeUpper}, new Fields("line"));
        Pipe grpByEvery = new Every(upperGroupBy, new Count(), new Fields("line"));

        Pipe grpByEveryE1 = new Each(grpByEvery, new Fields("line"), new Identity(new Fields("line")));
        Pipe grpByEveryE2 = new Each(grpByEvery, new Fields("line"), new Identity(new Fields("linegrpE2")));

        Pipe cogroup = new CoGroup("cogroup", pipeLower, new Fields("lineLower"), grpByEveryE1, new Fields("line"));
        Each cogroupEach = new Each(cogroup, new Fields("line"), new Identity(new Fields("line")));

        Pipe cogroupNested = new CoGroup(cogroupEach, new Fields("line"), grpByEveryE2, new Fields("linegrpE2"));
        Pipe cogrpNestedEach = new Each(cogroupNested, new Fields("line"), new Identity(new Fields("line")));

        Flow flow = getPlatform().getFlowConnector().connect(sources, sink, cogrpNestedEach);
        flow.complete();
    }
}
