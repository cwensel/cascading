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

package cascading.tap;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import cascading.ClusterTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.MultiMapReducePlanner;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class TapTest extends ClusterTestCase implements Serializable
  {
  String inputFileComments = "build/test/data/comments+lower.txt";
  String inputFileJoined = "build/test/data/lower+upper.txt";
  String inputFileCross = "build/test/data/lhs+rhs-cross.txt";
  String inputFileUpper = "build/test/data/upper.txt";
  String inputFileLower = "build/test/data/lower.txt";

  String outputPath = "build/test/output/tap/";

  public TapTest()
    {
    super( "tap tests", true );
    }

  public void testDfs() throws URISyntaxException, IOException
    {
    Tap tap = new Dfs( new Fields( "foo" ), "some/path" );

    assertTrue( "wrong scheme", tap.getQualifiedPath( MultiMapReducePlanner.getJobConf( getProperties() ) ).toUri().getScheme().equalsIgnoreCase( "hdfs" ) );

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

  public void testS3fs() throws URISyntaxException, IOException
    {
    // don't test qualified path, it tries to connect to s3 service

    new S3fs( new Fields( "foo" ), "s3://localhost:5001/some/path" );
    new S3fs( new Fields( "foo" ), new URI( "s3://localhost:5001/some/path" ) );

    try
      {
      new S3fs( new Fields( "foo" ), "hdfs://localhost:5001/some/path" );
      fail( "not valid url" );
      }
    catch( Exception exception )
      {
      }

    try
      {
      new S3fs( new Fields( "foo" ), new URI( "hdfs://localhost:5001/some/path" ) );
      fail( "not valid url" );
      }
    catch( Exception exception )
      {
      }
    }

  public void testLfs() throws URISyntaxException, IOException
    {
    Tap tap = new Lfs( new Fields( "foo" ), "some/path" );

    assertTrue( "wrong scheme", tap.getQualifiedPath( MultiMapReducePlanner.getJobConf( getProperties() ) ).toUri().getScheme().equalsIgnoreCase( "file" ) );

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
    public Tuple source( Object key, Object value )
      {
      if( value.toString().matches( "^\\s*#.*$" ) )
        return null;

      return super.source( key, value );
      }
    }

  public void testNullsFromScheme() throws IOException
    {
    if( !new File( inputFileComments ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileComments );

    Tap source = new Hfs( new CommentScheme( new Fields( "line" ) ), inputFileComments );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Identity() );

    Tap sink = new Hfs( new TextLine( 1 ), outputPath + "/testnulls", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

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
    if( !new File( inputFileJoined ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileJoined );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileJoined );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "number", "lower", "upper" ), "\t" ) );

    Tap sink = new Hfs( new TextLine( 1 ), outputPath + "/testtemplates", true );

    sink = new TemplateTap( (Hfs) sink, "%s-%s", 1 );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    Tap test = new Hfs( new TextLine( 1 ), sink.getPath().toString() + "/1-a" );
    validateLength( flow.openTapForRead( test ), 1 );

    test = new Hfs( new TextLine( 1 ), sink.getPath().toString() + "/2-b" );
    validateLength( flow.openTapForRead( test ), 1 );
    }

  public void testTemplateTapView() throws IOException
    {
    if( !new File( inputFileJoined ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileJoined );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileJoined );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "number", "lower", "upper" ), "\t" ) );

    Tap sink = new Hfs( new SequenceFile( new Fields( "upper" ) ), outputPath + "/testtemplatesview", true );

    sink = new TemplateTap( (Hfs) sink, "%s-%s", new Fields( "number", "lower" ), 1 );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    Tap test = new Hfs( new SequenceFile( new Fields( "upper" ) ), sink.getPath().toString() + "/1-a" );
    validateLength( flow.openTapForRead( test ), 1, 1 );

    test = new Hfs( new SequenceFile( new Fields( "upper" ) ), sink.getPath().toString() + "/2-b" );
    validateLength( flow.openTapForRead( test ), 1, 1 );

    TupleEntryIterator input = flow.openTapForRead( test ); // open 2-b

    assertEquals( "wrong value", "B", input.next().get( 0 ) );

    input.close();
    }

  public void testSinkDeclaredFields() throws IOException
    {
    if( !new File( inputFileCross ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileCross );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileCross );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third" ), "\\s" ), Fields.ALL );

    Tap sink = new Hfs( new TextLine( new Fields( "line" ), new Fields( "second", "first", "third" ) ), outputPath + "/declaredsinks", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "declaredsinks.dot" );

    flow.complete();

    validateLength( flow, 37, null );

    TupleEntryIterator iterator = flow.openSink();

    String line = iterator.next().getString( 0 );
    assertTrue( "not equal: wrong values", line.matches( "[a-z]\t[0-9]\t[A-Z]" ) );

    iterator.close();
    }

  public void testMultiSinkTap() throws IOException
    {
    if( !new File( inputFileJoined ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileJoined );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileJoined );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "number", "lower", "upper" ), "\t" ) );

    Tap lhsSink = new Hfs( new TextLine( new Fields( "offset", "line" ), new Fields( "number", "lower" ) ), outputPath + "/multisink/lhs", SinkMode.REPLACE );
    Tap rhsSink = new Hfs( new TextLine( new Fields( "offset", "line" ), new Fields( "number", "upper" ) ), outputPath + "/multisink/rhs", SinkMode.REPLACE );

    Tap sink = new MultiSinkTap( lhsSink, rhsSink );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow.openTapForRead( lhsSink ), 5 );
    validateLength( flow.openTapForRead( rhsSink ), 5 );
    }

  public void testGlobHfs() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap source = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), "build/test/data/?{ppe[_r],owe?}.txt" );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/glob/", true );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), "\\s" );
    Pipe concatPipe = new Each( new Pipe( "concat" ), new Fields( "line" ), splitter );

    Flow concatFlow = new FlowConnector( getProperties() ).connect( "first", source, sink, concatPipe );

    Tap nextSink = new Hfs( new TextLine(), outputPath + "/glob2/", true );

    Flow nextFlow = new FlowConnector( getProperties() ).connect( "second", sink, nextSink, concatPipe );

    Cascade cascade = new CascadeConnector().connect( concatFlow, nextFlow );

    cascade.complete();

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    validateLength( concatFlow, 10, null );
    }

  public void testMultiSourceIterator() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Tap source = new MultiSourceTap( sourceLower, sourceUpper );

    validateLength( source.openForRead( new JobConf() ), 10, null );
    }
  }
