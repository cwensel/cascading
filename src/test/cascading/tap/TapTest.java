/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.MultiMapReducePlanner;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
public class TapTest extends ClusterTestCase implements Serializable
  {
  String inputFileComments = "build/test/data/comments+lower.txt";
  String inputFileJoined = "build/test/data/lower+upper.txt";

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

    sink = new TemplateTap( (Hfs) sink, "%s-%s" );

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

    sink = new TemplateTap( (Hfs) sink, "%s-%s", new Fields( "number", "lower" ) );

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
  }
