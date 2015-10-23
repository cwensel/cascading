package cascading.tupleserialization;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.pipe.CoGroup;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.joiner.Joiner;
import cascading.pipe.joiner.MixedJoin;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tupleserialization.ImplicitBigDecimalType;
import cascading.tuple.hadoop.BigDecimalSerialization;
import cascading.tuple.hadoop.TupleSerializationProps;
import static data.InputData.textDelimitedAccount;

/**
 * The test case throws below error-
 * cascading.CascadingException: unable to load serializer for: cascading.platform.hadoop2.ImplicitBigDecimalType from: org.apache.hadoop.io.serializer.SerializationFactory
 * 
 * @author pushpakg
 *
 */
public class TestSerializationIssue extends PlatformTestCase {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5102827093582362753L;

	@Test
	public void testSerializationIssue() throws IOException {
		System.setProperty("platform.includes", "hadoop2-mr1");
		
		getPlatform().copyFromLocal(textDelimitedAccount);
		Properties props = new Properties();

		TupleSerializationProps.addSerialization(props,
				BigDecimalSerialization.class.getName());
		
		getPlatform().getProperties().putAll(props);
		
		/*
		 * Could not replicate the issue with below commented code.
		 * 
		 * */
	/*	Tap source = getPlatform().getDelimitedFile(
				new Fields("account_no", "name"), true, ",", null,
				new Class[] { ImplicitBigDecimalType.class, String.class },
				textDelimitedAccount, SinkMode.KEEP);

		Tap sink = getPlatform().getDelimitedFile(
				new Fields("a.account_no", "b.account_no", "a.sum"),
				true, ",", null,
				new Class[] { ImplicitBigDecimalType.class, ImplicitBigDecimalType.class, Double.class },
				getOutputPath("account_output"), SinkMode.REPLACE);
		*/
		/*
		 * The issue is replicated when taps are defined like below
		 * 
		 * */
		Tap source = new Hfs(new TextDelimited(new Fields("account_no", "name"), true, ",", null, 
				new Class[] { ImplicitBigDecimalType.class, String.class } ), textDelimitedAccount);

		Tap sink = new Hfs(new TextDelimited(new Fields("a.account_no",
				"b.account_no", "a.sum"), true, ",", null, 
				new Class[] { ImplicitBigDecimalType.class, ImplicitBigDecimalType.class, Double.class } ), 
				getOutputPath("account_output"), SinkMode.REPLACE);
		

		Pipe inputPipe = new Pipe("Input Pipe");
		Fields account_no = new Fields("account_no", ImplicitBigDecimalType.class);

		Pipe groupByPipe = new GroupBy("GroupBy Pipe", inputPipe, account_no);

		Count count = new Count(new Fields("count").applyTypes(ImplicitBigDecimalType.class));

		Pipe countPipe = new Every(groupByPipe, new Fields("name"), count, Fields.ALL);

		Pipe p1 = new Pipe("Pipe P1", countPipe);
		Pipe p2 = new Pipe("Pipe P2", countPipe);

		p1 = new Rename(p1, new Fields("account_no", "count"), new Fields("a.account_no", "a.count"));
		p2 = new Rename(p2, new Fields("account_no", "count"), new Fields("b.account_no", "b.count"));

		Pipe p3 = new Pipe("Join P3", p1);
		Pipe p4 = new Pipe("Join P4", p2);

		Pipe[] joinPipes = new Pipe[] { p3, p4 };
		boolean[] joinType = new boolean[] { true, true };
		Joiner joiner = new MixedJoin(joinType);

		Pipe cogroupPipe = new CoGroup(joinPipes,
				new Fields[] { new Fields("a.account_no"), new Fields("b.account_no") },
				new Fields("a.account_no", "a.count", "b.account_no", "b.count"),
				joiner);

		Sum sum = new Sum(new Fields("a.sum"));

		Pipe sumPipe = new Every(cogroupPipe, new Fields("a.count"), sum,
				Fields.ALL);

		Flow flow = getPlatform().getFlowConnector().connect(source, sink, sumPipe);

		flow.complete();

		List<Tuple> results = asList(flow, sink, new Fields("a.sum"));
		assertTrue(results.contains(new Tuple(3.0)));
	}
}