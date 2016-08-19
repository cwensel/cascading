package cascading.flow.hadoop;

import cascading.PlatformTestCase;
import cascading.flow.hadoop.planner.rule.expression.BalanceHashJoinExpression;
import cascading.flow.iso.NonTap;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.iso.finder.GraphFinder;
import cascading.flow.planner.iso.finder.Match;
import cascading.flow.planner.iso.transformer.ContractedTransformer;
import cascading.operation.Function;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.platform.TestPlatform;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static data.InputData.inputFileLower;
import static data.InputData.inputFileUpper;

public class ExpressionFinderPlatformTest extends PlatformTestCase {

    // test HashJoin graph with a rhs which is a Hfs
    public static class HashJoinGraphHfsRHS extends FlowElementGraph
    {
        public HashJoinGraphHfsRHS(TestPlatform platform, String outputPath) throws Exception
        {
            platform.copyFromLocal( inputFileLower );
            platform.copyFromLocal( inputFileUpper );

            Tap sourceLower = platform.getTextFile( new Fields( "offset", "line" ), inputFileLower );
            Tap sourceUpper = platform.getTextFile( new Fields( "offset", "line" ), inputFileUpper );

            Map<String, Tap> sources = new HashMap<>();

            sources.put( "lower", sourceLower );
            sources.put( "upper", sourceUpper );

            Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

            Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
            Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

            Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

            Tap sink = platform.getTextFile( new Fields( "line" ), outputPath, SinkMode.REPLACE );
            Map<String, Tap> sinks = new HashMap<>();
            sinks.put(splice.getName(), sink);

            initialize( sources, sinks, splice );
        }
    }

    // test HashJoin graph with a rhs which is a Hfs
    public static class HashJoinGraphNonTapRHS extends FlowElementGraph
    {
        public HashJoinGraphNonTapRHS(TestPlatform platform, String outputPath) throws Exception
        {
            platform.copyFromLocal( inputFileLower );
            platform.copyFromLocal( inputFileUpper );

            Tap sourceLower = new NonTap( new Fields( "offset", "line" ));
            Tap sourceUpper = new NonTap( new Fields( "offset", "line" ));

            Map<String, Tap> sources = new HashMap<>();

            sources.put( "lower", sourceLower );
            sources.put( "upper", sourceUpper );

            Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

            Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
            Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

            Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

            Tap sink = platform.getTextFile( new Fields( "line" ), outputPath, SinkMode.REPLACE );
            Map<String, Tap> sinks = new HashMap<>();
            sinks.put(splice.getName(), sink);

            initialize( sources, sinks, splice );
        }
    }

    // test to see if we're able to match a graph that has a hashJoin rhs that is a Hfs
    @Test
    public void testFindHfsRHS() throws Exception
    {
        ElementGraph graph = new HashJoinGraphHfsRHS(getPlatform(), getOutputPath());
        BalanceHashJoinExpression hashJoinExpression = new BalanceHashJoinExpression();

        graph = new ContractedTransformer( hashJoinExpression.getContractionExpression() ).transform( graph ).getEndGraph();

        GraphFinder graphFinder = new GraphFinder( hashJoinExpression.getMatchExpression() );

        Match match = graphFinder.findFirstMatch( graph );

        Assert.assertTrue(match.foundMatch());
    }

    // test to see if we end up skipping to match a graph that has a hashJoin rhs that isn't a hfs
    @Test
    public void testFailedFindNonHfsRHS() throws Exception
    {
        ElementGraph graph = new HashJoinGraphNonTapRHS(getPlatform(), getOutputPath());
        BalanceHashJoinExpression hashJoinExpression = new BalanceHashJoinExpression();

        graph = new ContractedTransformer( hashJoinExpression.getContractionExpression() ).transform( graph ).getEndGraph();

        GraphFinder graphFinder = new GraphFinder( hashJoinExpression.getMatchExpression() );

        Match match = graphFinder.findFirstMatch( graph );

        Assert.assertFalse(match.foundMatch());
    }
}
