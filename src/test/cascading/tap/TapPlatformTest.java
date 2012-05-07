/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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
import java.util.regex.Pattern;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.hadoop.util.Hadoop18TapUtil;
import cascading.test.HadoopPlatform;
import cascading.test.LocalPlatform;
import cascading.test.PlatformRunner;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import org.junit.Test;

import static data.InputData.*;

/**
 *
 */
@PlatformRunner.Platform({LocalPlatform.class, HadoopPlatform.class})
public class TapPlatformTest extends PlatformTestCase implements Serializable
  {
  public TapPlatformTest()
    {
    super( true );
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

    Tap sink = getPlatform().getDelimitedFile( Fields.UNKNOWN, getOutputPath( "unknownsinks" ), SinkMode.REPLACE );

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
    Tap rhsSink = getPlatform().getTextFile( new Fields( "offset", "line" ), new Fields( "number", "upper" ), getOutputPath( "/multisink/rhs" ), SinkMode.REPLACE );

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
  public void testTemplateTap() throws IOException
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileJoined );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "number", "lower", "upper" ), "\t" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "testtemplates" ), SinkMode.REPLACE );

    sink = getPlatform().getTemplateTap( sink, "%s-%s", 1 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    Tap test = getPlatform().getTextFile( sink.getIdentifier().toString() + "/1-a" );
    validateLength( flow.openTapForRead( test ), 1 );

    test = getPlatform().getTextFile( sink.getIdentifier().toString() + "/2-b" );
    validateLength( flow.openTapForRead( test ), 1 );

    if( getPlatform() instanceof HadoopPlatform )
      {
      String stringPath = sink.getIdentifier().toString() + "/1-a/" + Hadoop18TapUtil.TEMPORARY_PATH;
      assertFalse( flow.resourceExists( getPlatform().getTextFile( stringPath ) ) );

      stringPath = sink.getIdentifier().toString() + "/2-b/" + Hadoop18TapUtil.TEMPORARY_PATH;
      assertFalse( flow.resourceExists( getPlatform().getTextFile( stringPath ) ) );
      }
    }

  @Test
  public void testTemplateTapTextDelimited() throws IOException
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileJoined );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "number", "lower", "upper" ), "\t" ) );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "number", "lower", "upper" ), "+", getOutputPath( "testdelimitedtemplates" ), SinkMode.REPLACE );

    sink = getPlatform().getTemplateTap( sink, "%s-%s", 1 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    Tap test = getPlatform().getTextFile( new Fields( "line" ), sink.getIdentifier().toString() + "/1-a" );
    validateLength( flow.openTapForRead( test ), 1, Pattern.compile( "[0-9]\\+[a-z]\\+[A-Z]" ) );

    test = getPlatform().getTextFile( new Fields( "line" ), sink.getIdentifier().toString() + "/2-b" );
    validateLength( flow.openTapForRead( test ), 1, Pattern.compile( "[0-9]\\+[a-z]\\+[A-Z]" ) );
    }

  @Test
  public void testTemplateTapView() throws IOException
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileJoined );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "number", "lower", "upper" ), "\t" ) );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "upper" ), getOutputPath( "testtemplatesview" ), SinkMode.REPLACE );

    sink = getPlatform().getTemplateTap( sink, "%s-%s", new Fields( "number", "lower" ), 1 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    Tap test = getPlatform().getDelimitedFile( new Fields( "upper" ), sink.getIdentifier().toString() + "/1-a", SinkMode.KEEP );
    validateLength( flow.openTapForRead( test ), 1, 1 );

    test = getPlatform().getDelimitedFile( new Fields( "upper" ), sink.getIdentifier().toString() + "/2-b", SinkMode.KEEP );
    validateLength( flow.openTapForRead( test ), 1, 1 );

    TupleEntryIterator input = flow.openTapForRead( test ); // open 2-b

    assertEquals( "wrong value", "B", input.next().get( 0 ) );

    input.close();
    }
  }
