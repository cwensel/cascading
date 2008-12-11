/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Insert;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleIterator;
import cascading.tuple.TupleEntryIterator;

/** @version $Id: //depot/calku/cascading/src/test/cascading/FieldedPipesTest.java#4 $ */
public class BufferPipesTest extends ClusterTestCase
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

  String outputPath = "build/test/output/buffer/";

  public BufferPipesTest()
    {
    super( "buffer pipes", false ); // no need for clustering
    }

  public void testSimpleBuffer() throws Exception
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLhs );
    Tap sink = new Hfs( new TextLine(), outputPath + "/simple", true );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "num" ) );

    pipe = new Every( pipe, new TestBuffer( new Fields( "next" ), 2, true, true, "next" ) );

    pipe = new Each( pipe, new Insert( new Fields( "final" ), "final" ), Fields.ALL );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "unknownselect.dot" );

    flow.complete();

    validateLength( flow, 23, null );

    TupleEntryIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\tnull\tnext\tfinal", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "1\ta\tnext\tfinal", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "1\tb\tnext\tfinal", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "1\tc\tnext\tfinal", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "1\tnull\tnext\tfinal", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testSimpleBuffer2() throws Exception
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLhs );
    Tap sink = new Hfs( new TextLine(), outputPath + "/simple", true );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "num" ) );

    pipe = new Every( pipe, new Fields( "lower" ), new TestBuffer( new Fields( "next" ), 1, true, "next" ), Fields.RESULTS );

    pipe = new Each( pipe, new Insert( new Fields( "final" ), "final" ), Fields.ALL );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "unknownselect.dot" );

    flow.complete();

    validateLength( flow, 18, null );

    TupleEntryIterator iterator = flow.openSink();

    Comparable line = iterator.next().get( 1 );
    assertEquals( "not equal: tuple.get(1)", "next\tfinal", line );
    line = iterator.next().get( 1 );
    assertEquals( "not equal: tuple.get(1)", "next\tfinal", line );
    line = iterator.next().get( 1 );
    assertEquals( "not equal: tuple.get(1)", "next\tfinal", line );

    iterator.close();
    }
  }