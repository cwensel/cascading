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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 *
 */
public class HttpFileSystem extends FileSystem
  {
  public static final String HTTP_SCHEME = "http";
  public static final String HTTPS_SCHEME = "https";

  static
    {
    HttpURLConnection.setFollowRedirects( true );
    }

  private String scheme;

  public void initialize( URI uri, Configuration configuration ) throws IOException
    {
    setConf( configuration );

    scheme = uri.getScheme();
    }

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

  public FSDataInputStream open( Path path, int i ) throws IOException
    {
    final URL url = path.toUri().toURL();

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod( "GET" );
    connection.connect();

    final InputStream in = connection.getInputStream();

    return new FSDataInputStream( new FSInputStream()
    {
    public int read() throws IOException
      {
      return in.read();
      }

    public int read( byte[] b, int off, int len ) throws IOException
      {
      return in.read( b, off, len );
      }

    public void close() throws IOException
      {
      in.close();
      }

    public void seek( long pos ) throws IOException
      {
      throw new IOException( "Can't seek!" );
      }

    public long getPos() throws IOException
      {
      throw new IOException( "Position unknown!" );
      }

    public boolean seekToNewSource( long targetPos ) throws IOException
      {
      return false;
      }
    } );
    }

  public FSDataOutputStream create( Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable ) throws IOException
    {
    throw new UnsupportedOperationException( "not supported" );
    }

  public boolean rename( Path path, Path path1 ) throws IOException
    {
    throw new UnsupportedOperationException( "not supported" );
    }

  public boolean delete( Path path ) throws IOException
    {
    throw new UnsupportedOperationException( "not supported" );
    }

  public boolean exists( Path path ) throws IOException
    {
    URL url = path.toUri().toURL();

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod( "HEAD" );
    connection.connect();

    System.out.println( "connection.getResponseCode() = " + connection.getResponseCode() );
    System.out.println( "connection.getResponseMessage() = " + connection.getResponseMessage() );

    return connection.getResponseCode() == 200;
    }

  public FileStatus[] listStatus( Path path ) throws IOException
    {
    return new FileStatus[0];
    }

  public void setWorkingDirectory( Path path )
    {
    throw new UnsupportedOperationException( "not supported" );
    }

  public Path getWorkingDirectory()
    {
    throw new UnsupportedOperationException( "not supported" );
    }

  public boolean mkdirs( Path path, FsPermission fsPermission ) throws IOException
    {
    throw new UnsupportedOperationException( "not supported" );
    }

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
