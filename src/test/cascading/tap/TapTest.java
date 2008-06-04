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
