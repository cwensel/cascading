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
import java.net.URI;

import cascading.util.S3Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

/**
 *
 */
public class S3HttpFileSystem extends FileSystem
  {
  public static final String S3HTTP_SCHEME = "s3http";

  private URI uri;
  private RestS3Service s3Service;
  private S3Bucket s3Bucket;


  public void initialize( URI uri, Configuration configuration ) throws IOException
    {
    setConf( configuration );

    this.s3Service = S3Util.getS3Service( uri );
    this.s3Bucket = S3Util.getS3Bucket( uri );
    this.uri = URI.create( uri.getScheme() + "://" + uri.getAuthority() );
    }

  public URI getUri()
    {
    return uri;
    }

  public FSDataInputStream open( Path path, int i ) throws IOException
    {
    S3Object object = S3Util.getObject( s3Service, s3Bucket, path, S3Util.Request.OBJECT );
    final InputStream in = S3Util.getObjectInputStream( object );

    // ctor requires Seekable or PositionedReadable stream
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
      throw new IOException( "not supported" );
      }

    public long getPos() throws IOException
      {
      throw new IOException( "not supported" );
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
    return S3Util.getObject( s3Service, s3Bucket, path, S3Util.Request.DETAILS ) != null;
    }

  public FileStatus[] listStatus( Path path ) throws IOException
    {
    return new FileStatus[]{getFileStatus( path )};
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
    S3Object object = S3Util.getObject( s3Service, s3Bucket, path, S3Util.Request.DETAILS );

    if( LOG.isDebugEnabled() )
      LOG.debug( "returning status for: " + path );

    return new FileStatus( object.getContentLength(), false, 1, getDefaultBlockSize(), object.getLastModifiedDate().getTime(), path );
    }
  }
