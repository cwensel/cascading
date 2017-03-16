package cascading.flow.hadoop;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.LockingFlowListener;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.util.Util;

import static data.InputData.inputFileLower;
import static data.InputData.inputFileUpper;

/**
 * To demonstrate PartitionNodes difference between MR and Tez
 * In Hadoop and Tez this results in a single PartitionStep.
 * Where the two differ is Hadoop partitions into 2 nodes. First node (map) reads from the two sourceTaps, performs the pipeLeft & pipeRight transforms
 * Second node (reduce) does the merge, each, groupBy, every, every, each, each
 * In case of Tez, we have 4 nodes. First two are equivalent to the Hadoop node 1, read from tap + each
 * Next node just does the merge, each, groupBy. Node after that does the every, every, each, each.
 * As we don't have the aggregations following the groupBy on the same nodes, we end up streaming a lot of tuples to disk.
 */
public class PartitionNodesTest extends PlatformTestCase {

  private static final Logger LOG = LoggerFactory.getLogger( PartitionNodesTest.class );

  public PartitionNodesTest()
  {
    super( true ); // must be run in cluster mode
  }

  @Test
  public void testGraphDifference() throws Exception
  {
    if( !getPlatform().isUseCluster() )
      return;

    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLeft = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceRight = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );
    Map sources = new HashMap();
    sources.put( "left", sourceLeft );
    sources.put( "right", sourceRight );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    Pipe pipeLeft = new Each( new Pipe( "left" ), new Fields( "line" ), splitter );
    Pipe pipeRight = new Each( new Pipe( "right" ), new Fields( "line" ), splitter );

    Pipe merge = new Merge("merge", pipeLeft, pipeRight);
    Pipe mergeEach = new Each(merge, new Identity());
    Pipe groupBy = new GroupBy(mergeEach, new Fields("num"));
    Pipe ev1 = new Every(groupBy, new Average());
    Pipe ev2 = new Every(ev1, new Count());
    Pipe evEach = new Each(ev2, new Identity());
    Pipe evEach2 = new Each(evEach, new Identity());

    Tap sink = new Hfs( new TextLine(), getOutputPath( "result" ), SinkMode.REPLACE );

    final Flow flow = getPlatform().getFlowConnector( getProperties() ).connect( sources, sink, evEach2 );

    final LockingFlowListener listener = new LockingFlowListener();

    flow.addListener( listener );

    LOG.info( "calling start" );
    flow.start();

    Util.safeSleep( 90000 );

    assertTrue( "did not start", listener.started.tryAcquire( 60, TimeUnit.SECONDS ) );
  }
}
