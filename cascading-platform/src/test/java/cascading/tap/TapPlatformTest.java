/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.tap;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import cascading.PlatformTestCase;
import cascading.TestBuffer;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowTapException;
import cascading.operation.Debug;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.partition.DelimitedPartition;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIteratorProps;
import org.junit.Test;

import static data.InputData.*;

/**
 *
 */
public class TapPlatformTest extends PlatformTestCase implements Serializable
  {
  public TapPlatformTest()
    {
    super( true, 4, 1 );
    }

  @Test
  public void testSinkDeclaredFields() throws IOException
    {
    getPlatform().copyFromLocal( inputFileCross );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileCross );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third" ), "\\s" ), Fields.ALL );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), new Fields( "second", "first", "third" ), getOutputPath( "declaredsinks" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 37, null );

    TupleEntryIterator iterator = flow.openSink();

    String line = iterator.next().getString( 0 );
    assertTrue( "not equal: wrong values", line.matches( "[a-z]\t[0-9]\t[A-Z]" ) );

    iterator.close();
    }

  @Test
  public void testSinkUnknown() throws IOException
    {
    getPlatform().copyFromLocal( inputFileCross );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileCross );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third" ), "\\s" ), Fields.RESULTS );

    Tap sink = getPlatform().getTabDelimitedFile( Fields.UNKNOWN, getOutputPath( "unknownsinks" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 37, null );

    TupleEntryIterator iterator = flow.openSink();

    String line = iterator.next().getTuple().toString();
    assertTrue( "not equal: wrong values: " + line, line.matches( "[0-9]\t[a-z]\t[A-Z]" ) );

    iterator.close();
    }

  @Test
  public void testMultiSinkTap() throws IOException
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileJoined );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "number", "lower", "upper" ), "\t" ) );

    Tap lhsSink = getPlatform().getTextFile( new Fields( "offset", "line" ), new Fields( "number", "lower" ), getOutputPath( "multisink/lhs" ), SinkMode.REPLACE );
    Tap rhsSink = getPlatform().getTextFile( new Fields( "offset", "line" ), new Fields( "number", "upper" ), getOutputPath( "multisink/rhs" ), SinkMode.REPLACE );

    Tap sink = new MultiSinkTap( lhsSink, rhsSink );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow.openTapForRead( lhsSink ), 5 );
    validateLength( flow.openTapForRead( rhsSink ), 5 );
    }

  @Test
  public void testMultiSourceIterator() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Tap source = new MultiSourceTap( sourceLower, sourceUpper );

    validateLength( source.openForRead( getPlatform().getFlowProcess() ), 10 );
    }

  @Test
  public void testSideFileCollector() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getTextFile( inputFileLhs );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "sidefile/direct" ), SinkMode.REPLACE );

    Tap sideFile = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "sidefile/indirect" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "num" ) );

    pipe = new Every( pipe, new TestBuffer( sideFile, new Fields( "next" ), 2, true, true, "next" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 23, null );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "1\tnull\tnext" ) ) );
    assertTrue( results.contains( new Tuple( "1\ta\tnext" ) ) );
    assertTrue( results.contains( new Tuple( "1\tb\tnext" ) ) );
    assertTrue( results.contains( new Tuple( "1\tc\tnext" ) ) );
    assertTrue( results.contains( new Tuple( "1\tnull\tnext" ) ) );

    results = asList( flow, sideFile );

    assertEquals( 13, results.size() );

    assertTrue( results.contains( new Tuple( "1\ta" ) ) );
    assertTrue( results.contains( new Tuple( "1\tb" ) ) );
    assertTrue( results.contains( new Tuple( "1\tc" ) ) );
    }

  @Test
  public void testPartitionTapTextDelimited() throws IOException
    {
    runPartitionTest( "" ); // empty string is treated as null
    }

  @Test
  public void testPartitionTapTextDelimitedPostFix() throws IOException
    {
    runPartitionTest( "/somepath/filename.txt" );
    }

  private void runPartitionTest( String postfix ) throws IOException
    {
    getPlatform().copyFromLocal( inputFileCrossX2 );

    Tap source = getPlatform().getDelimitedFile( new Fields( "number", "lower", "upper" ), " ", inputFileCrossX2 );

    Tap partitionTap = getPlatform().getDelimitedFile( new Fields( "upper" ), "+", getOutputPath( "/partitioned" ), SinkMode.REPLACE );

    Partition partition = new DelimitedPartition( new Fields( "lower", "number" ), "/", postfix );
    partitionTap = getPlatform().getPartitionTap( partitionTap, partition, 1 );

    Flow firstFlow = getPlatform().getFlowConnector().connect( source, partitionTap, new Pipe( "partition" ) );

    firstFlow.complete();

    Tap sink = getPlatform().getDelimitedFile( new Fields( "number", "lower", "upper" ), "+", getOutputPath( "/final" ), SinkMode.REPLACE );

    Flow secondFlow = getPlatform().getFlowConnector().connect( partitionTap, sink, new Pipe( "copy" ) );

    secondFlow.complete();

    Tap test = getPlatform().getTextFile( new Fields( "line" ), partitionTap.getIdentifier().toString() + "/a/1" + postfix );
    validateLength( firstFlow.openTapForRead( test ), 6, Pattern.compile( "[A-Z]" ) );

    test = getPlatform().getTextFile( new Fields( "line" ), partitionTap.getIdentifier().toString() + "/b/2" + postfix );
    validateLength( firstFlow.openTapForRead( test ), 6, Pattern.compile( "[A-Z]" ) );

    List<Tuple> tuples = asList( firstFlow, partitionTap );

    assertTrue( tuples.contains( new Tuple( "A", "a", "1" ) ) );
    assertTrue( tuples.contains( new Tuple( "B", "b", "2" ) ) );

    test = getPlatform().getTextFile( new Fields( "line" ), sink.getIdentifier() );
    validateLength( secondFlow.openTapForRead( test ), 74, Pattern.compile( "[0-9]\\+[a-z]\\+[A-Z]" ) );
    }

  @Test
  public void testTupleEntrySchemeIteratorExceptionHandling() throws IOException
    {
    if( getPlatformName().equals( "local" ) )
      return;  // no gzip support

    getPlatform().copyFromLocal( inputFileUnexpectedEndOfFile );

    Tap source = getPlatform().getTextFile( inputFileUnexpectedEndOfFile );
    Tap sink = getPlatform().getTextFile( getOutputPath( getTestName() ), SinkMode.REPLACE );

    Map<Object, Object> properties = getProperties();

    TupleEntrySchemeIteratorProps.setPermittedExceptions( properties, java.io.EOFException.class );

    Pipe pipe = new Pipe( "data" );
    pipe = new Each( pipe, new Identity() );

    FlowDef flowDef = FlowDef.flowDef().addSource( pipe, source ).addTailSink( pipe, sink );
    Flow flow = getPlatform().getFlowConnector( properties ).connect( flowDef );
    flow.complete();
    validateLength( flow.openSink(), 307 );
    }

  @Test
  public void testTupleEntrySchemeIteratorEOFException() throws IOException
    {
    if( getPlatformName().equals( "local" ) )
      return;  // no gzip support

    getPlatform().copyFromLocal( inputFileUnexpectedEndOfFile );

    Tap source = getPlatform().getTextFile( inputFileUnexpectedEndOfFile );
    Tap sink = getPlatform().getTextFile( getOutputPath( getTestName() ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "data" );
    pipe = new Each( pipe, new Identity() );

    FlowDef flowDef = FlowDef.flowDef().addSource( pipe, source ).addTailSink( pipe, sink );
    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    try
      {
      flow.complete();
      fail( "flow should have thrown an Exception" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  @Test(expected = FlowTapException.class)
  public void testTapKeep() throws IOException
    {
    getPlatform().copyFromLocal( inputFileCrossX2 );

    Tap source = getPlatform().getDelimitedFile( new Fields( "number", "lower", "upper" ), " ", inputFileCrossX2 );

    String outputPath = getOutputPath( "/sink" );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "upper" ), "+", outputPath, SinkMode.REPLACE );

    Flow firstFlow = getPlatform().getFlowConnector().connect( "first", source, sink, new Pipe( "head" ) );

    firstFlow.complete();

    sink = getPlatform().getDelimitedFile( new Fields( "upper" ), "+", outputPath, SinkMode.KEEP );

    Flow secondFlow = getPlatform().getFlowConnector().connect( "second", source, sink, new Each( new Pipe( "head" ), new Debug() ) );

    secondFlow.complete();
    }

  @Test(expected = FlowTapException.class)
  public void testPartitionTapKeep() throws IOException
    {
    getPlatform().copyFromLocal( inputFileCrossX2 );

    Tap source = getPlatform().getDelimitedFile( new Fields( "number", "lower", "upper" ), " ", inputFileCrossX2 );

    String outputPath = getOutputPath( "/partitioned" );
    Tap partitionTap = getPlatform().getDelimitedFile( new Fields( "upper" ), "+", outputPath, SinkMode.REPLACE );

    Partition partition = new DelimitedPartition( new Fields( "lower", "number" ) );
    partitionTap = getPlatform().getPartitionTap( partitionTap, partition, 1 );

    Flow firstFlow = getPlatform().getFlowConnector().connect( "first", source, partitionTap, new Pipe( "partition" ) );

    firstFlow.complete();

    partitionTap = getPlatform().getDelimitedFile( new Fields( "upper" ), "+", outputPath, SinkMode.KEEP );
    partitionTap = getPlatform().getPartitionTap( partitionTap, partition, 1 );

    Flow secondFlow = getPlatform().getFlowConnector().connect( "second", source, partitionTap, new Pipe( "partition" ) );

    secondFlow.complete();
    }
  }
