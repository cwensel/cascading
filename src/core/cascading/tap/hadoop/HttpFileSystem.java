/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

/**
 * Class HttpFileSystem provides a basic read-only {@link FileSystem} for accessing remote HTTP and HTTPS data.
 * <p/>
 * To use this FileSystem, just use regular http:// or https:// URLs.
 */
public class HttpFileSystem extends StreamedFileSystem
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( HttpFileSystem.class );

  /** Field HTTP_SCHEME */
  public static final String HTTP_SCHEME = "http";
  /** Field HTTPS_SCHEME */
  public static final String HTTPS_SCHEME = "https";

  static
    {
    HttpURLConnection.setFollowRedirects( true );
    }

  /** Field scheme */
  private String scheme;
  /** Field authority */
  private String authority;

  @Override
  public void initialize( URI uri, Configuration configuration ) throws IOException
    {
    setConf( configuration );

    scheme = uri.getScheme();
    authority = uri.getAuthority();
    }

  @Override
  public URI getUri()
    {
    try
      {
      return new URI( scheme, authority, null, null, null );
      }
    catch( URISyntaxException exception )
      {
      throw new RuntimeException( "failed parsing uri", exception );
      }
    }

  @Override
  public FileStatus[] globStatus( Path path, PathFilter pathFilter ) throws IOException
    {
    FileStatus fileStatus = getFileStatus( path );

    if( fileStatus == null )
      return null;

    return new FileStatus[]{fileStatus};
    }

  @Override
  public FSDataInputStream open( Path path, int i ) throws IOException
    {
    URL url = makeUrl( path );

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod( "GET" );
    connection.connect();

    debugConnection( connection );

    return new FSDataInputStream( new FSDigestInputStream( connection.getInputStream(), getMD5SumFor( getConf(), path ) ) );
    }

  @Override
  public boolean exists( Path path ) throws IOException
    {
    URL url = makeUrl( path );

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod( "HEAD" );
    connection.connect();

    debugConnection( connection );

    return connection.getResponseCode() == 200;
    }

  @Override
  public FileStatus getFileStatus( Path path ) throws IOException
    {
    URL url = makeUrl( path );

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod( "HEAD" );
    connection.connect();

    debugConnection( connection );

    if( connection.getResponseCode() != 200 )
      throw new FileNotFoundException( "could not find file: " + path );

    long length = connection.getHeaderFieldInt( "Content-Length", 0 );

    length = length < 0 ? 0 : length; // queries may return -1

    long modified = connection.getHeaderFieldDate( "Last-Modified", System.currentTimeMillis() );

    return new FileStatus( length, false, 1, getDefaultBlockSize(), modified, path );
    }

  private void debugConnection( HttpURLConnection connection ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      {
      LOG.debug( "connection.getURL() = " + connection.getURL() );
      LOG.debug( "connection.getRequestMethod() = " + connection.getRequestMethod() );
      LOG.debug( "connection.getResponseCode() = " + connection.getResponseCode() );
      LOG.debug( "connection.getResponseMessage() = " + connection.getResponseMessage() );
      LOG.debug( "connection.getContentLength() = " + connection.getContentLength() );
      }
    }

  private URL makeUrl( Path path ) throws IOException
    {
    if( path.toString().startsWith( scheme ) )
      return URI.create( path.toString() ).toURL();

    try
      {
      return new URI( scheme, authority, path.toString(), null, null ).toURL();
      }
    catch( URISyntaxException exception )
      {
      throw new IOException( exception.getMessage() );
      }
    }
  }
