/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.tap.hadoop.io;

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

  @Deprecated
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

  public FSDataOutputStream append( Path f, int bufferSize, Progressable progress ) throws IOException
    {
    throw new UnsupportedOperationException( "not supported" );
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
