/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/** Class HttpFileSystem provides a basic read-only {@link FileSystem} for accessing remote HTTP and HTTPS data. */
public class HttpFileSystem extends StreamedFileSystem
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( HttpFileSystem.class );

  public static final String HTTP_SCHEME = "http";
  public static final String HTTPS_SCHEME = "https";

  static
    {
    HttpURLConnection.setFollowRedirects( true );
    }

  private String scheme;

  @Override
  public void initialize( URI uri, Configuration configuration ) throws IOException
    {
    setConf( configuration );

    scheme = uri.getScheme();
    }

  @Override
  public URI getUri()
    {
    try
      {
      return new URI( scheme + ":///" );
      }
    catch( URISyntaxException exception )
      {
      throw new RuntimeException( "failed parsing uri", exception );
      }
    }

  @Override
  public FSDataInputStream open( Path path, int i ) throws IOException
    {
    URL url = path.toUri().toURL();

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod( "GET" );
    connection.connect();

    return new FSDataInputStream( new FSDigestInputStream( connection.getInputStream(), getMD5SumFor( getConf(), path ) ) );
    }

  @Override
  public boolean exists( Path path ) throws IOException
    {
    URL url = path.toUri().toURL();

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod( "HEAD" );
    connection.connect();

    if( LOG.isDebugEnabled() )
      {
      LOG.debug( "connection.getResponseCode() = " + connection.getResponseCode() );
      LOG.debug( "connection.getResponseMessage() = " + connection.getResponseMessage() );
      }

    return connection.getResponseCode() == 200;
    }

  @Override
  public FileStatus getFileStatus( Path path ) throws IOException
    {
    URL url = path.toUri().toURL();

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod( "HEAD" );
    connection.connect();

    // Content-Length
    long length = connection.getHeaderFieldInt( "Content-Length", 0 );
    // Last-Modified
    long modified = connection.getHeaderFieldDate( "Last-Modified", System.currentTimeMillis() );

    return new FileStatus( length, false, 1, getDefaultBlockSize(), modified, path );
    }
  }
