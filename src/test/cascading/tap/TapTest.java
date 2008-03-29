/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.tap;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import cascading.ClusterTestCase;
import cascading.tuple.Fields;

/**
 *
 */
public class TapTest extends ClusterTestCase
  {
  public TapTest()
    {
    super( "tap tests", true );
    }

  public void testDfs() throws URISyntaxException, IOException
    {
    Tap tap = new Dfs( new Fields( "foo" ), "some/path" );

    assertTrue( "wrong scheme", tap.getQualifiedPath( jobConf ).toUri().getScheme().equalsIgnoreCase( "hdfs" ) );

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

    assertTrue( "wrong scheme", tap.getQualifiedPath( jobConf ).toUri().getScheme().equalsIgnoreCase( "file" ) );

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
  }
