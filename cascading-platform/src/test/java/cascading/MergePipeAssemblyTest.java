package cascading;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.operation.aggregator.First;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import static data.InputData.testDelimitedFile1;
import static data.InputData.testDelimitedFile2;


public class MergePipeAssemblyTest extends PlatformTestCase {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Test
	public void testMergePipeAssembly() throws IOException{
		
		System.setProperty( "platform.includes", "hadoop2-mr1");
	
		getPlatform().copyFromLocal(testDelimitedFile1);
		getPlatform().copyFromLocal(testDelimitedFile2);
		
		Tap source1 = getPlatform().getDelimitedFile(
  				new Fields("f1", "f2", "f3"), false, "|", null,
  				new Class[] { String.class, String.class, String.class }, testDelimitedFile1,
  				SinkMode.KEEP);
		
		Tap source2 = getPlatform().getDelimitedFile(
  				new Fields("f1", "f2", "f3"), false, "|", null,
  				new Class[] { String.class, String.class, String.class }, testDelimitedFile2,
  				SinkMode.KEEP);
		
		Tap sink = getPlatform().getDelimitedFile(
  				new Fields("f1", "f2", "f3"), false, "|", null,
  				new Class[] { String.class, String.class, String.class },
  				getOutputPath("mergepipeassemblytest_output"), SinkMode.REPLACE);
		
		Pipe inputPipe1 = new Pipe("Input Pipe 1");
		Pipe inputPipe2 = new Pipe("Input Pipe 2");
		
		Fields f1 = new Fields("f1");
		
		Pipe groupByPipe1 = new GroupBy("GroupBy Pipe 1", inputPipe1, f1);
		Pipe groupByPipe2 = new GroupBy("GroupBy Pipe 2", inputPipe2, f1);
		
		First firstTuple = new First();
		
		Pipe everyPipe1 = new Every(groupByPipe1, firstTuple, Fields.RESULTS);
		Pipe everyPipe2 = new Every(groupByPipe2, firstTuple, Fields.RESULTS);
		
		Pipe mergePipe = new Merge("Merge Pipe", everyPipe1, everyPipe2);
		
		FlowDef flowDef = FlowDef.flowDef().addSource(inputPipe1, source1)
				.addSource(inputPipe2, source2).addTailSink(mergePipe, sink);

		Flow flow = getPlatform().getFlowConnector().connect(flowDef);
		
		flow.complete();
		
		List<Tuple> results = asList( flow, sink);
		
		assertTrue(results.contains(new Tuple("CCC","ASD","DFG")));
		assertTrue(results.contains(new Tuple("NNN","IUY","OWER")));
	}
}