/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

import cascading.PlatformTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.HadoopPlanner;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RecordReader;

import static data.InputData.*;

/**
 *
 */
@PlatformTest(platforms = {"hadoop"})
public class HadoopTapsTest extends PlatformTestCase implements Serializable
  {
  public HadoopTapsTest()
    {
    super( true );
    }

  public void testDfs() throws URISyntaxException, IOException
    {
    Tap tap = new Dfs( new Fields( "foo" ), "some/path" );

    String path = tap.getFullIdentifier( HadoopPlanner.getJobConf( getProperties() ) );
    assertTrue( "wrong scheme", new Path( path ).toUri().getScheme().equalsIgnoreCase( "hdfs" ) );

    new Dfs( new Fields( "foo" ), "hdfs://localhost:5001/some/path" );
    new Dfs( new Fields( "foo" ), new URI( "hdfs://localhost:5001/some/path" ) );

    try
      {
      new Dfs( new Fields( "foo" ), "s3://localhost:5001/some/path" );
      fail( "not valid url" );
      }
    catch( Exception exception )
      {
      }

    try
      {
      new Dfs( new Fields( "foo" ), new URI( "s3://localhost:5001/some/path" ) );
      fail( "not valid url" );
      }
    catch( Exception exception )
      {
      }
    }

  public void testLfs() throws URISyntaxException, IOException
    {
    Tap tap = new Lfs( new Fields( "foo" ), "some/path" );

    String path = tap.getFullIdentifier( HadoopPlanner.getJobConf( getProperties() ) );
    assertTrue( "wrong scheme", new Path( path ).toUri().getScheme().equalsIgnoreCase( "file" ) );

    new Lfs( new Fields( "foo" ), "file:///some/path" );

    try
      {
      new Lfs( new Fields( "foo" ), "s3://localhost:5001/some/path" );
      fail( "not valid url" );
      }
    catch( Exception exception )
      {
      }
    }

  public class CommentScheme extends TextLine
    {
    public CommentScheme()
      {
      }

    public CommentScheme( Fields sourceFields )
      {
      super( sourceFields );
      }

    @Override
    public boolean source( HadoopFlowProcess flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
      {
      boolean success = sourceCall.getInput().next( sourceCall.getContext()[ 0 ], sourceCall.getContext()[ 1 ] );

      if( !success )
        return false;

      if( sourceCall.getContext()[ 1 ].toString().matches( "^\\s*#.*$" ) )
        return source( flowProcess, sourceCall );

      sourceHandleInput( sourceCall );

      return true;
      }
    }

  public void testNullsFromScheme() throws IOException
    {
    getPlatform().copyFromLocal( inputFileComments );

    Tap source = new Hfs( new CommentScheme( new Fields( "line" ) ), inputFileComments );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Identity() );

    Tap sink = new Hfs( new TextLine( 1 ), getOutputPath( "testnulls" ), SinkMode.REPLACE );

    Flow flow = new HadoopFlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, null );

    TupleEntryIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1 a", iterator.next().get( 1 ) );

    iterator.close();

    // confirm the tuple iterator can handle nulls from the source
    validateLength( flow.openSource(), 5 );
    }

  public void testTemplateTap() throws IOException
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileJoined );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "number", "lower", "upper" ), "\t" ) );

    Tap sink = new Hfs( new TextLine( 1 ), getOutputPath( "testtemplates" ), SinkMode.REPLACE );

    sink = new TemplateTap( (Hfs) sink, "%s-%s", 1 );

    Flow flow = new HadoopFlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    Tap test = new Hfs( new TextLine( 1 ), sink.getIdentifier().toString() + "/1-a" );
    validateLength( flow.openTapForRead( test ), 1 );

    test = new Hfs( new TextLine( 1 ), sink.getIdentifier().toString() + "/2-b" );
    validateLength( flow.openTapForRead( test ), 1 );
    }

  public void testTemplateTapTextDelimited() throws IOException
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileJoined );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "number", "lower", "upper" ), "\t" ) );

    Tap sink = new Hfs( new TextDelimited( new Fields( "number", "lower", "upper" ), "+" ), getOutputPath( "testdelimitedtemplates" ), SinkMode.REPLACE );

    sink = new TemplateTap( (Hfs) sink, "%s-%s", 1 );

    Flow flow = new HadoopFlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    Tap test = new Hfs( new TextLine( new Fields( "line" ) ), sink.getIdentifier().toString() + "/1-a" );
    validateLength( flow.openTapForRead( test ), 1, Pattern.compile( "[0-9]\\+[a-z]\\+[A-Z]" ) );

    test = new Hfs( new TextLine( new Fields( "line" ) ), sink.getIdentifier().toString() + "/2-b" );
    validateLength( flow.openTapForRead( test ), 1, Pattern.compile( "[0-9]\\+[a-z]\\+[A-Z]" ) );
    }

  public void testTemplateTapView() throws IOException
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileJoined );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "number", "lower", "upper" ), "\t" ) );

    Tap sink = new Hfs( new SequenceFile( new Fields( "upper" ) ), getOutputPath( "testtemplatesview" ), SinkMode.REPLACE );

    sink = new TemplateTap( (Hfs) sink, "%s-%s", new Fields( "number", "lower" ), 1 );

    Flow flow = new HadoopFlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    Tap test = new Hfs( new SequenceFile( new Fields( "upper" ) ), sink.getIdentifier().toString() + "/1-a" );
    validateLength( flow.openTapForRead( test ), 1, 1 );

    test = new Hfs( new SequenceFile( new Fields( "upper" ) ), sink.getIdentifier().toString() + "/2-b" );
    validateLength( flow.openTapForRead( test ), 1, 1 );

    TupleEntryIterator input = flow.openTapForRead( test ); // open 2-b

    assertEquals( "wrong value", "B", input.next().get( 0 ) );

    input.close();
    }

  public void testGlobHfs() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    String dataLocation = System.getProperty( data.InputData.TEST_DATA_PATH, "src/test/data/" );

    GlobHfs source = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), dataLocation + "?{ppe[_r],owe?}.txt" );

    assertEquals( 2, source.getTaps().length );

    // show globhfs will just match a directory if ended with a /
    assertEquals( 1, new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), dataLocation + "../?ata/" ).getTaps().length );

    Tap sink = new Hfs( new TextLine(), getOutputPath( "glob" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), "\\s" );
    Pipe concatPipe = new Each( new Pipe( "concat" ), new Fields( "line" ), splitter );

    Flow concatFlow = new HadoopFlowConnector( getProperties() ).connect( "first", source, sink, concatPipe );

    Tap nextSink = new Hfs( new TextLine(), getOutputPath( "glob2" ), SinkMode.REPLACE );

    Flow nextFlow = new HadoopFlowConnector( getProperties() ).connect( "second", sink, nextSink, concatPipe );

    Cascade cascade = new CascadeConnector().connect( concatFlow, nextFlow );

    cascade.complete();

    validateLength( concatFlow, 10 );
    }

  public void testNestedMultiSourceGlobHfs() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    String dataLocation = System.getProperty( data.InputData.TEST_DATA_PATH, "src/test/data/" );

    GlobHfs source1 = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), dataLocation + "?{ppe[_r]}.txt" );
    GlobHfs source2 = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), dataLocation + "?{owe?}.txt" );

    MultiSourceTap source = new MultiSourceTap( source1, source2 );

    assertEquals( 2, source.getChildTaps().length );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), getOutputPath( "globmultisource" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), "\\s" );
    Pipe concatPipe = new Each( new Pipe( "concat" ), new Fields( "line" ), splitter );

    Flow concatFlow = new HadoopFlowConnector( getProperties() ).connect( "first", source, sink, concatPipe );

    Tap nextSink = new Hfs( new TextLine(), getOutputPath( "globmultiource2" ), SinkMode.REPLACE );

    Flow nextFlow = new HadoopFlowConnector( getProperties() ).connect( "second", sink, nextSink, concatPipe );

    Cascade cascade = new CascadeConnector().connect( concatFlow, nextFlow );

    cascade.complete();

    validateLength( concatFlow, 10 );
    }

  public void testMultiSourceIterator() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    String dataLocation = System.getProperty( data.InputData.TEST_DATA_PATH, "src/test/data/" );

    GlobHfs source1 = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), dataLocation + "?{ppe[_r]}.txt" );
    GlobHfs source2 = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), dataLocation + "?{owe?}.txt" );

    MultiSourceTap source = new MultiSourceTap( source1, source2 );

    validateLength( source.openForRead( getPlatform().getFlowProcess() ), 10 );

    GlobHfs sourceMulti = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), dataLocation + "?{ppe[_r],owe?}.txt" );

    source = new MultiSourceTap( sourceMulti );

    validateLength( source.openForRead( getPlatform().getFlowProcess() ), 10, null );
    }
  }
