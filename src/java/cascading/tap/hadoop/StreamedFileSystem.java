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

package cascading.tap.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/** Class StreamedFileSystem is a base class for {@link FileSystem} implementations that manage remote resources. */
public abstract class StreamedFileSystem extends FileSystem
  {
  @Override
  public FSDataOutputStream create( Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress ) throws IOException
    {
    throw new UnsupportedOperationException( "not supported" );
    }

  @Override
  public boolean rename( Path path, Path path1 ) throws IOException
    {
    throw new UnsupportedOperationException( "not supported" );
    }

  @Override
  public boolean delete( Path path ) throws IOException
    {
    throw new UnsupportedOperationException( "not supported" );
    }

  @Override
  public boolean delete( Path path, boolean b ) throws IOException
    {
    throw new UnsupportedOperationException( "not supported" );
    }

  @Override
  public Path getWorkingDirectory()
    {
    return new Path( "/" ).makeQualified( this );
    }

  @Override
  public void setWorkingDirectory( Path f )
    {
    }

  @Override
  public boolean mkdirs( Path path, FsPermission fsPermission ) throws IOException
    {
    throw new UnsupportedOperationException( "not supported" );
    }

  @Override
  public FileStatus[] listStatus( Path path ) throws IOException
    {
    return new FileStatus[]{getFileStatus( path )};
    }

  public static String getMD5SumFor( Configuration conf, Path path )
    {
    return getMD5SumFor( conf, path.toString() );
    }

  public static String getMD5SumFor( Configuration conf, String path )
    {
    return conf.get( path + ".md5" );
    }

  public static void setMD5SumFor( Configuration conf, Path path, String md5Hex )
    {
    setMD5SumFor( conf, path.toString(), md5Hex );
    }

  public static void setMD5SumFor( Configuration conf, String path, String md5Hex )
    {
    if( md5Hex == null || md5Hex.length() == 0 )
      return;

    conf.set( path + ".md5", md5Hex );
    }
  }
