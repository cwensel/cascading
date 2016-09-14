package cascading.pipe.HashJoinIssue;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

public class HashJoinIssueTest {

	@Test
	public void itShouldTestNumberOfFieldsAfterHashJoin() throws IOException
	{
		Fields fields=new Fields("eid","ename","Deptno").applyTypes(Integer.class,String.class,Integer.class);
		Scheme sourceSch = new TextDelimited(fields, true, "|");
		Scheme sinkSch = new TextDelimited(fields, true, "|");
		Tap source = new Hfs(sourceSch, "Input/lookupInput.txt");
		Tap sink = new Hfs(sinkSch,"lookup",SinkMode.REPLACE);
		
		Pipe p1 = new Pipe("first");
		Pipe p2 = new Pipe("second");
		Pipe [] mergePipe= {p1,p2};
		Pipe gather = new Merge("gather", mergePipe);
		
		gather = new Rename(gather, fields, new Fields("0.eid","0.ename","0.Deptno"));
		
		Pipe p3 =  new Pipe("third");
		p3 = new Rename(p3, fields, new Fields("1.eid","1.ename","1.Deptno"));
		
		Pipe lookup[] = {gather,p3}; 
	
		Fields[] joinFields= new Fields[2];
		joinFields[0]= new Fields("0.Deptno");
		joinFields[1]= new Fields("1.Deptno");
		Fields outputFields = new Fields("0.eid","0.ename","0.Deptno")
				.append(new Fields("1.eid","1.ename","1.Deptno"));
		Pipe lookupJoin= new HashJoin("Join", lookup, joinFields, outputFields, new InnerJoin());
		
		
		Pipe retain = new Retain(lookupJoin, new Fields("0.eid","1.ename","1.Deptno"));
		
		Pipe out = new Rename(retain, new Fields("0.eid","1.ename","1.Deptno"), fields );
		
		FlowDef flowDef = FlowDef.flowDef()
				.addSource(p1, source)
				.addSource(p2, source)
				.addSource(p3, source)
				.addTailSink(out, sink);
		
		Flow flow = new Hadoop2MR1FlowConnector().connect(flowDef);
		flow.complete();
		
		TupleEntryIterator tupleEntryIterator =flow.openSink();
		Fields fieldsOut = tupleEntryIterator.getFields();
		
		Assert.assertEquals(fields.size(), fieldsOut.size());
		
	}
	
}
