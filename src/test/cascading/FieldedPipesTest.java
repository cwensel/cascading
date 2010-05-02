/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Debug;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.filter.And;
import cascading.operation.function.UnGroup;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.InnerJoin;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

public class FieldedPipesTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.10.txt";
  String inputFileIps = "build/test/data/ips.20.txt";
  String inputFileNums20 = "build/test/data/nums.20.txt";
  String inputFileNums10 = "build/test/data/nums.10.txt";
  String inputFileCritics = "build/test/data/critics.txt";

  String inputFileUpper = "build/test/data/upper.txt";
  String inputFileLower = "build/test/data/lower.txt";
  String inputFileLowerOffset = "build/test/data/lower-offset.txt";
  String inputFileJoined = "build/test/data/lower+upper.txt";
  String inputFileJoinedExtra = "build/test/data/extra+lower+upper.txt";

  String inputFileLhs = "build/test/data/lhs.txt";
  String inputFileRhs = "build/test/data/rhs.txt";
  String inputFileCross = "build/test/data/lhs+rhs-cross.txt";

  String outputPath = "build/test/output/fields/";

  public FieldedPipesTest()
    {
    super( "fielded pipes", true ); // leave cluster testing enabled
    }

  public void testSimpleGroup() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextLine(), outputPath + "/simple", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "groupcount.dot" );

    flow.complete();

    validateLength( flow.openSource(), 10 ); // validate source, this once, as a sanity check
    validateLength( flow, 8, null );
    }

  public void testSimpleChain() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count( new Fields( "count1" ) ) );
    pipe = new Every( pipe, new Count( new Fields( "count2" ) ) );
    pipe = new Every( pipe, new Count( new Fields( "count3" ) ) );
    pipe = new Every( pipe, new Count( new Fields( "count4" ) ) );

    Tap sink = new Hfs( new SequenceFile( Fields.ALL ), outputPath + "/simplechain", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "chainedevery.dot" );

    flow.complete();

    validateLength( flow, 8, 5 );
    }

  public void testChainEndingWithEach() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count( new Fields( "count1" ) ) );
    pipe = new Every( pipe, new Count( new Fields( "count2" ) ) );

    pipe = new Each( pipe, new Fields( "count1", "count2" ), new ExpressionFunction( new Fields( "sum" ), "count1 + count2", int.class ), Fields.ALL );
    Tap sink = new Hfs( new TextLine(), outputPath + "/chaineach", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "chainedevery.dot" );

    flow.complete();

    validateLength( flow, 8, null );
    }

  // also tests the RegexSplitter

  public void testNoGroup() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( "\\s+" ), new Fields( 1 ) );

    Tap sink = new Hfs( new TextLine( 1 ), outputPath + "/simplesplit", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10, null );

    TupleEntryIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "75.185.76.245", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testCopy() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    Tap sink = new Hfs( new TextLine( 1 ), outputPath + "/copy", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10, null );
    }

  public void testSimpleMerge() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/merge/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new GroupBy( "merge", Pipe.pipes( pipeLower, pipeUpper ), new Fields( "num" ), null, false );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 10, null );

    TupleEntryIterator iterator = flow.openSink();

    Comparable line = iterator.next().get( 1 );
    assertTrue( "not equal: tuple.get(1)", line.equals( "1\ta" ) || line.equals( "1\tA" ) );
    line = iterator.next().get( 1 );
    assertTrue( "not equal: tuple.get(1)", line.equals( "1\ta" ) || line.equals( "1\tA" ) );

    iterator.close();
    }

  /**
   * Specifically tests GroupBy will return the correct grouping fields to the following Every
   *
   * @throws Exception
   */
  public void testSimpleMergeThree() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );
    copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );
    Tap sourceLowerOffset = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/mergethree/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), splitter );

    Pipe splice = new GroupBy( "merge", Pipe.pipes( pipeLower, pipeUpper, pipeOffset ), new Fields( "num" ) );

    splice = new Every( splice, new Fields( "char" ), new First( new Fields( "first" ) ) );

    splice = new Each( splice, new Fields( "num", "first" ), new Identity() );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 6, null );
    }


  public void testUnGroup() throws Exception
    {
    if( !new File( inputFileJoined ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileJoined );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileJoined );
    Tap sink = new Hfs( new TextLine(), outputPath + "/ungrouped", true );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ) ) );

    pipe = new Each( pipe, new UnGroup( new Fields( "num", "char" ), new Fields( "num" ), Fields.fields( new Fields( "lower" ), new Fields( "upper" ) ) ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "ungroup.dot" );

    flow.complete();

    validateLength( flow, 10, null );
    }

  public void testUnGroupBySize() throws Exception
    {
    if( !new File( inputFileJoinedExtra ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileJoinedExtra );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileJoinedExtra );
    Tap sink = new Hfs( new TextLine(), outputPath + "/ungrouped_size", true );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num1", "num2", "lower", "upper" ) ) );

    pipe = new Each( pipe, new UnGroup( new Fields( "num1", "num2", "char" ), new Fields( "num1", "num2" ), 1 ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "ungroup.dot" );

    flow.complete();

    validateLength( flow, 10, null );

    TupleEntryIterator iterator = flow.openSink();

    Comparable line = iterator.next().get( 1 );
    assertTrue( "not equal: tuple.get(1)", line.equals( "1\t1\ta" ) );
    line = iterator.next().get( 1 );
    assertTrue( "not equal: tuple.get(1)", line.equals( "1\t1\tA" ) );

    iterator.close();

    }

  public void testFilter() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/filter", true );

    Pipe pipe = new Pipe( "test" );

    Filter filter = new RegexFilter( "^68.*" );

    pipe = new Each( pipe, new Fields( "line" ), filter );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "flow.dot" );

    flow.complete();

    validateLength( flow, 3, null );
    }

  public void testLogicFilter() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/logicfilter", true );

    Pipe pipe = new Pipe( "test" );

    Filter filter = new And( new RegexFilter( "^68.*$" ), new RegexFilter( "^1000.*$" ) );

    pipe = new Each( pipe, new Fields( "line" ), filter );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "flow.dot" );

    flow.complete();

    validateLength( flow, 3, null );
    }

  public void testFilterComplex() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/filtercomplex", true );

    Pipe pipe = new Pipe( "test" );

//    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new Each( pipe, new Fields( "line" ), TestConstants.APACHE_COMMON_PARSER );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );
    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "method" ), new Identity( new Fields( "value" ) ), Fields.ALL );

    pipe = new GroupBy( pipe, new Fields( "value" ) );

    pipe = new Every( pipe, new Count(), new Fields( "value", "count" ) );


    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "filter.dot" );

    flow.complete();

    validateLength( flow, 1, null );
    }

  /**
   * Intentionally filters all values out to test next mr job behaves
   *
   * @throws Exception
   */
  public void testFilterAll() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/filtercomplex", true );

    Pipe pipe = new Pipe( "test" );

    String regex = "^([^ ]*) +[^ ]* +[^ ]* +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";
    Fields fieldDeclaration = new Fields( "ip", "time", "method", "event", "status", "size" );
    int[] groups = {1, 2, 3, 4, 5, 6};
    RegexParser function = new RegexParser( fieldDeclaration, regex, groups );
    pipe = new Each( pipe, new Fields( "line" ), function );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^fobar" ) ); // intentionally filtering all

    pipe = new GroupBy( pipe, new Fields( "method" ) );

    pipe = new Each( pipe, new Fields( "method" ), new Identity( new Fields( "value" ) ), Fields.ALL );

    pipe = new GroupBy( pipe, new Fields( "value" ) );

    pipe = new Every( pipe, new Count(), new Fields( "value", "count" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "filter.dot" );

    flow.complete();

    validateLength( flow, 0, null );
    }

//  public void testLimitFilter() throws Exception
//    {
//    if( !new File( inputFileApache ).exists() )
//      fail( "data file not found" );
//
//    copyFromLocal( inputFileApache );
//
//    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
//    Tap sink = new Lfs( new TextLine(), outputPath + "/limitfilter", true );
//
//    Pipe pipe = new Pipe( "test" );
//
//    Filter filter = new Limit( 7 );
//
//    pipe = new Each( pipe, new Fields( "line" ), filter );
//
//    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );
//
////    flow.writeDOT( "flow.dot" );
//
//    flow.complete();
//
//    validateLength( flow, 7, null );
//    }

  //

  public void testCross() throws Exception
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );
    copyFromLocal( inputFileRhs );

    Map sources = new HashMap();

    sources.put( "lhs", new Hfs( new TextLine(), inputFileLhs ) );
    sources.put( "rhs", new Hfs( new TextLine(), inputFileRhs ) );

    Pipe pipeLower = new Each( "lhs", new Fields( "line" ), new RegexSplitter( new Fields( "numLHS", "charLHS" ), " " ) );
    Pipe pipeUpper = new Each( "rhs", new Fields( "line" ), new RegexSplitter( new Fields( "numRHS", "charRHS" ), " " ) );

    Pipe cross = new CoGroup( pipeLower, new Fields( "numLHS" ), pipeUpper, new Fields( "numRHS" ), new InnerJoin() );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cross/", true );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sink, cross );

//    System.out.println( "flow =\n" + flow );

    flow.complete();

    validateLength( flow, 37, null );

    TupleEntryIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tB", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testSplit() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    // 46 192

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink1 = new Hfs( new TextLine(), outputPath + "/split1", true );
    Tap sink2 = new Hfs( new TextLine(), outputPath + "/split2", true );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Map sources = new HashMap();
    sources.put( "split", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sinks, left, right );

//    flow.writeDOT( "split.dot" );

    flow.complete();

    validateLength( flow, 1, "left" );
    validateLength( flow, 2, "right" );
    }

  /**
   * verifies non-safe rules apply in the proper place
   *
   * @throws Exception
   */
  public void testSplitNonSafe() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    // 46 192

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink1 = new Hfs( new TextLine(), outputPath + "/nonsafesplit1", true );
    Tap sink2 = new Hfs( new TextLine(), outputPath + "/nonsafesplit2", true );

    Pipe pipe = new Pipe( "split" );

    // run job on non-safe operation, forces 3 mr jobs.
    pipe = new Each( pipe, new TestFunction( new Fields( "ignore" ), new Tuple( 1 ), false ), new Fields( "line" ) );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Map sources = new HashMap();
    sources.put( "split", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sinks, left, right );

//    flow.writeDOT( "split.dot" );

    flow.complete();

    validateLength( flow, 1, "left" );
    validateLength( flow, 2, "right" );
    }

  public void testSplitSameSourceMerged() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    // 46 192

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/splitsourcemerged", true );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Pipe merged = new GroupBy( "merged", Pipe.pipes( left, right ), new Fields( "line" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, merged );

//    flow.writeDOT( "splitmerged.dot" );

    flow.complete();

    validateLength( flow, 3 );
    }

  /**
   * verifies not inserting Identity between groups works
   *
   * @throws Exception
   */
  public void testSplitOut() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "num", "line" ) ), inputFileApache );

    Map sources = new HashMap();

    sources.put( "lower1", sourceLower );

    // using null pos so all fields are written
    Tap sink1 = new Hfs( new TextLine(), outputPath + "/splitout1", true );
    Tap sink2 = new Hfs( new TextLine(), outputPath + "/splitout2", true );

    Map sinks = new HashMap();

    sinks.put( "output1", sink1 );
    sinks.put( "output2", sink2 );

    Pipe pipeLower1 = new Pipe( "lower1" );

    Pipe left = new GroupBy( "output1", pipeLower1, new Fields( 0 ) );
    Pipe right = new GroupBy( "output2", left, new Fields( 0 ) );

    Flow flow = new FlowConnector().connect( sources, sinks, Pipe.pipes( left, right ) );

//    flow.writeDOT( "splitout.dot" );

    flow.complete();

    validateLength( flow, 10, "output1" );
    validateLength( flow, 10, "output2" );
    }

  public void testSplitComplex() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    // 46 192

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink1 = new Hfs( new TextLine(), outputPath + "/splitcomp1", true );
    Tap sink2 = new Hfs( new TextLine(), outputPath + "/splitcomp2", true );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Fields( "ip" ), new Count(), new Fields( "ip", "count" ) );

    pipe = new Each( pipe, new Fields( "ip" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "ip" ), new RegexFilter( ".*46.*" ) );

    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "ip" ), new RegexFilter( ".*102.*" ) );

    Map sources = Cascades.tapsMap( "split", source );
    Map sinks = Cascades.tapsMap( Pipe.pipes( left, right ), Tap.taps( sink1, sink2 ) );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sinks, left, right );

//    flow.writeDOT( "splitcomplex.dot" );

    flow.complete();

    validateLength( flow, 1, "left" );
    validateLength( flow, 1, "right" );
    }

  public void testConcatentation() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Tap source = new MultiSourceTap( sourceLower, sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/concat/", true );

    Pipe pipe = new Each( new Pipe( "concat" ), new Fields( "line" ), splitter );

    Pipe splice = new GroupBy( pipe, new Fields( "num" ) );

    Flow countFlow = new FlowConnector( getProperties() ).connect( source, sink, splice );

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 10, null );
    }

  public void testGeneratorAggregator() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new TestAggregator( new Fields( "count1" ), new Fields( "ip" ), new Tuple( "first1" ), new Tuple( "first2" ) ) );
    pipe = new Every( pipe, new TestAggregator( new Fields( "count2" ), new Fields( "ip" ), new Tuple( "second" ), new Tuple( "second2" ), new Tuple( "second3" ) ) );

    Tap sink = new Hfs( new TextLine(), outputPath + "/generatoraggregator", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8 * 2 * 3, null );
    }

  /**
   * If the sinks have the same scheme as a temp tap, replace the temp tap
   *
   * @throws Exception
   */
  public void testChainedTaps() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Each( new Pipe( "first" ), new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Each( new Pipe( "second", pipe ), new Fields( "ip" ), new RegexFilter( "7" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Each( new Pipe( "third", pipe ), new Fields( "ip" ), new RegexFilter( "6" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    String path = outputPath + "/chainedtaps/";
    Tap sinkFirst = new Hfs( new SequenceFile( new Fields( "ip" ) ), path + "first", true );
    Tap sinkSecond = new Hfs( new SequenceFile( new Fields( "ip" ) ), path + "second", true );
    Tap sinkThird = new Hfs( new SequenceFile( new Fields( "ip" ) ), path + "third", true );

    Map<String, Tap> sinks = Cascades.tapsMap( new String[]{"first", "second",
                                                            "third"}, Tap.taps( sinkFirst, sinkSecond, sinkThird ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sinks, pipe );

    assertEquals( "wrong number of steps", 3, flow.getSteps().size() );

//    flow.writeDOT( "chainedtaps.dot" );

    flow.complete();

    validateLength( flow, 3, null );
    }

  public void testReplace() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink = new Hfs( new TextLine( new Fields( "offset", "line" ), new Fields( "offset", "line" ) ), outputPath + "/replace", true );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( new Fields( 0 ), "^[^ ]*" );
    pipe = new Each( pipe, new Fields( "line" ), parser, Fields.REPLACE );
    pipe = new Each( pipe, new Fields( "line" ), new Identity( Fields.ARGS ), Fields.REPLACE );
    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "line" ) ), Fields.REPLACE );

    pipe = new Each( pipe, new Debug( true ) );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

//    flow.writeDOT( "simple.dot" );

    flow.complete();

    validateLength( flow, 10, 2, Pattern.compile( "^\\d+\\s\\d+\\s[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}$" ) );
    }

  public void testSwap() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink = new Hfs( new TextLine( new Fields( "offset", "line" ), new Fields( "count", "ipaddress" ) ), outputPath + "/swap", true );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( new Fields( "ip" ), "^[^ ]*" );
    pipe = new Each( pipe, new Fields( "line" ), parser, Fields.SWAP );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Fields( "ip" ), new Count( new Fields( "count" ) ) );
    pipe = new Each( pipe, new Fields( "ip" ), new Identity( new Fields( "ipaddress" ) ), Fields.SWAP );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

//    flow.writeDOT( "simple.dot" );

    flow.complete();

    validateLength( flow, 8, 2, Pattern.compile( "^\\d+\\s\\d+\\s[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}$" ) );
    }

  }
