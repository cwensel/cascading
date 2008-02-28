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


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * A section of an input file in zip format. Returned by
 * {@link ZipInputFormat#getSplits(org.apache.hadoop.mapred.JobConf , int)} and passed to
 * {@link ZipInputFormat#getRecordReader(org.apache.hadoop.mapred.InputSplit , org.apache.hadoop.mapred.JobConf , org.apache.hadoop.mapred.Reporter)}.
 */

public class ZipSplit implements InputSplit
  {

  public static final Log LOG = LogFactory
    .getLog( "org.apache.hadoop.mapred.ZipSplit" );

  private Path file;
  private String entryPath;
  private JobConf conf;
  private long length;

  ZipSplit()
    {
    }

  /**
   * Constructs a split from zip archive.
   *
   * @param file      the zip archive name
   * @param entryPath the path of the file to be read within the zip archive.
   * @param length    the uncompressed size of the file within the zip archive.
   * @param conf      see {@link org.apache.hadoop.mapred.JobConf}
   */
  public ZipSplit( Path file, String entryPath, long length, JobConf conf )
    {
    this.file = file;
    this.entryPath = entryPath;
    this.length = length;
    this.conf = conf;
    }

  /** The zip archive containing this split's data. */
  public Path getFile()
    {
    return file;
    }

  /** The path of the file within the zip archive. */
  public String getEntryPath()
    {
    return entryPath;
    }

  /** The uncompressed size of the file within the zip archive. */
  public long getLength() throws IOException
    {
    return length;
    }

  // //////////////////////////////////////////
  // Writable methods
  // //////////////////////////////////////////
  public void write( DataOutput out ) throws IOException
    {
    UTF8.writeString( out, file.toString() );
    UTF8.writeString( out, entryPath );
    out.writeLong( length );
    }

  public void readFields( DataInput in ) throws IOException
    {
    file = new Path( UTF8.readString( in ) );
    entryPath = UTF8.readString( in );
    length = in.readLong();
    }

  public String[] getLocations() throws IOException
    {
    String[][] hints = file.getFileSystem( conf ).getFileCacheHints( file, 0, length );
    if( hints != null && hints.length > 0 )
      {
      return hints[ 0 ];
      }
    return new String[]{};
    }

  }
