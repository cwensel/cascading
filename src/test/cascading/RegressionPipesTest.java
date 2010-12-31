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
import java.util.Map;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.Debug;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.assertion.AssertSizeMoreThan;
import cascading.operation.function.UnGroup;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

public class RegressionPipesTest extends ClusterTestCase
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

  String inputFileLhs = "build/test/data/lhs.txt";
  String inputFileRhs = "build/test/data/rhs.txt";
  String inputFileCross = "build/test/data/lhs+rhs-cross.txt";

  String outputPath = "build/test/output/regression/";

  public RegressionPipesTest()
    {
    super( "regression pipes", false );
    }


  /**
   * tests that a selector will select something other than the first position from an UNKNOWN tuple
   *
   * @throws Exception
   */
  public void testUnknown() throws Exception
    {
    if( !new File( inputFileJoined ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileJoined );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileJoined );
    Tap sink = new Hfs( new TextLine(), outputPath + "/unknown", true );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( Fields.UNKNOWN ) );

//    pipe = new Each( pipe, new Debug() );

    pipe = new Each( pipe, new Fields( 2 ), new Identity( new Fields( "label" ) ) );

//    pipe = new Each( pipe, new Debug() );

    pipe = new Each( pipe, new Fields( "label" ), new RegexFilter( "[A-Z]*" ) );

//    pipe = new Each( pipe, new Debug() );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "unknownselect.dot" );

    flow.complete();

    validateLength( flow, 5, null );
    }

  public void testCopy() throws Exception
    {
    if( !new File( inputFileJoined ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileJoined );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileJoined );
    Tap sink = new Hfs( new TextLine(), outputPath + "/copy", true );

    Pipe pipe = new Pipe( "test" );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "copy.dot" );

    flow.complete();

    validateLength( flow, 5, null );
    }

  /**
   * tests that a selector will select something other than the first position from an UNKNOWN tuple
   *
   * @throws Exception
   */
  public void testVarWidth() throws Exception
    {
    if( !new File( inputFileCritics ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileCritics );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileCritics );
    Tap sink = new Hfs( new TextLine(), outputPath + "/varwidth", true );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( Fields.UNKNOWN ) );

    pipe = new Each( pipe, AssertionLevel.STRICT, new AssertSizeMoreThan( 3 ) );

    pipe = new Each( pipe, new Fields( 0, 1, -1 ), new Identity( new Fields( "name", "second", "last" ) ) );

//    pipe = new Each( pipe, new Debug() );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "unknownselect.dot" );

    flow.complete();

    validateLength( flow, 7 );
    }

  /**
   * This test allows for Fields.UNKNOWN to propagate from the RegexSplitter through to the UnGroup (or any other
   * operation).
   * <p/>
   * This could be dangerous but feels very natural and part of the intentions of having UNKNOWN
   *
   * @throws Exception
   */
  public void testUnGroupUnknown() throws Exception
    {
    if( !new File( inputFileJoined ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileJoined );

    Tap source = new Hfs( new TextLine(), inputFileJoined );
    Tap sink = new Hfs( new TextLine(), outputPath + "/ungrouped-unknown", true );

    Pipe pipe = new Pipe( "test" );

    // emits Fields.UNKNOWN
    pipe = new Each( pipe, new Fields( 1 ), new RegexSplitter( "\t" ), Fields.ALL );

    // accepts Fields.UNKOWN
    pipe = new Each( pipe, new UnGroup( Fields.size( 2 ), new Fields( 0 ), Fields.fields( new Fields( 1 ), new Fields( 2 ) ) ) );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

//    flow.writeDOT( "ungroup.dot" );

    flow.complete();

    validateLength( flow, 10 );
    }

  public void testDupeHeadNames() throws Exception
    {
    // todo: re-enable these tests on next major release
    if( true )
      return;

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileJoined );
    Tap sink = new Hfs( new TextLine(), outputPath + "/unknown", true );

    Pipe lhs = new Pipe( "test" );

    lhs = new Each( lhs, new Fields( "line" ), new RegexSplitter( " " ) );

    Pipe rhs = new Pipe( "test" );

    rhs = new Each( rhs, new Fields( "line" ), new RegexSplitter( " " ) );

    Pipe group = new GroupBy( Pipe.pipes( lhs, rhs ), Fields.size( 3 ) );

    try
      {
      new FlowConnector( getProperties() ).connect( source, sink, group );
      fail( "did not fail on dupe head names" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  public void testDupeTailNames() throws Exception
    {
    // todo: re-enable these tests on next major release
    if( true )
      return;

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileJoined );
    Tap sink = new Hfs( new TextLine(), outputPath + "/unknown", true );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( " " ) );

    Pipe group = new GroupBy( pipe, Fields.size( 3 ) );

    Pipe lhs = new Pipe( "tail", group );
    lhs = new Each( group, new Fields( "line" ), new RegexSplitter( " " ) );

    Pipe rhs = new Pipe( "tail", group );
    rhs = new Each( group, new Fields( "line" ), new RegexSplitter( " " ) );

    Map<String, Tap> sinks = Cascades.tapsMap( Pipe.pipes( lhs, rhs ), Tap.taps( sink, sink ) );

    try
      {
      new FlowConnector( getProperties() ).connect( source, sinks, Pipe.pipes( lhs, rhs ) );
      fail( "did not fail on dupe head names" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  public void testIllegalCharsInTempFiles() throws Exception
    {
    if( !new File( inputFileJoined ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileJoined );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileJoined );
    Tap sink = new Hfs( new TextLine(), outputPath + "/illegalchars", true );

//    Pipe pipe = new Pipe( "bar:bar@foo://blah/\t(*(**^**&%&%^@#@&&() :::: ///\\\\ \t illegal chars in it" );
    Pipe pipe = new Pipe( "**&%&%bar:bar@foo://blah/\t(*(**^**&%&%^@#@&&() :::: ///\\\\ \t illegal chars in it" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( " " ) );

    pipe = new GroupBy( pipe, new Fields( 0 ) );

    pipe = new GroupBy( pipe, new Fields( 0 ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5 );
    }

  /**
   * Method testCoGroupSplitPipe tests the case where CoGroup on the lhs steps on the tuple as it passes down
   * the rhs. this is rare and expects that one side is all filters.
   *
   * @throws Exception when
   */
  public void testCoGroupSplitPipe() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileLower );
    Tap splitTap = new Hfs( new SequenceFile( new Fields( "num", "char" ) ), outputPath + "/complex/intermediate", SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe split = new Each( "split", splitter );

    Flow splitFlow = new FlowConnector( getProperties() ).connect( source, splitTap, split );

    splitFlow.complete();

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroupsplit/", true );

    Pipe lower = new Pipe( "lower" );

    Pipe lhs = new Pipe( "lhs", lower );

//    lhs = new Each( lhs, new Identity() ); // identity does not trigger the issue this tests.
//    lhs = new Each( lhs, new Debug( "lhs", true ) );

    Pipe rhs = new Pipe( "rhs", lower );

    rhs = new Each( rhs, new Debug( "rhs-pre", true ) );

    rhs = new Each( rhs, new Fields( "num" ), new Identity( new Fields( "num2" ) ) );

//    rhs = new Each( rhs, new Debug( "rhs-post", true ) );

    Pipe cogroup = new CoGroup( lhs, new Fields( "num" ), rhs, new Fields( "num2" ) );

//    cogroup = new Each( cogroup, new Debug( true ) );

    Flow flow = new FlowConnector( getProperties() ).connect( splitTap, sink, cogroup );

//    flow.writeDOT( "othercogroup.dot" );

    flow.complete();

    validateLength( flow, 5, null );

    TupleEntryIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb\t2", iterator.next().get( 1 ) );

    iterator.close();
    }

  /**
   * Method testCoGroupSplitPipe tests the case where GroupBy on the lhs steps on the tuple as it passes down
   * the rhs. this is rare and expects that one side is all filters.
   *
   * @throws Exception when
   */
  public void testGroupBySplitPipe() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileLower );
    Tap splitTap = new Hfs( new SequenceFile( new Fields( "num", "char" ) ), outputPath + "/complex/intermediate", SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe split = new Each( "split", splitter );

    Flow splitFlow = new FlowConnector( getProperties() ).connect( source, splitTap, split );

    splitFlow.complete();

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/groupbysplit/", true );

    Pipe lower = new Pipe( "lower" );

    Pipe lhs = new Pipe( "lhs", lower );

    Pipe rhs = new Pipe( "rhs", lower );

    rhs = new Each( rhs, new Fields( "num" ), new Identity( new Fields( "num2" ) ), new Fields( "num", "char" ) );


    Pipe groupBy = new GroupBy( Pipe.pipes( lhs, rhs ), new Fields( "num" ) );


    Flow flow = new FlowConnector( getProperties() ).connect( splitTap, sink, groupBy );

//    flow.writeDOT( "othercogroup.dot" );

    flow.complete();

    validateLength( flow, 10, null );

    TupleEntryIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "1\ta", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testLastEachNotModified() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new TestFunction( new Fields( "insert" ), new Tuple( "inserted" ) ) );

    pipe = new GroupBy( pipe, new Fields( "insert" ) );

    Tap sink = new Hfs( new TextLine(), outputPath + "/regression/lasteachmodified", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10, null );
    }
  }