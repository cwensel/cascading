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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

/**
 * A section of an input file in zip format. Returned by
 * {@link ZipInputFormat#getSplits(JobConf , int)} and passed to
 * {@link ZipInputFormat#getRecordReader(InputSplit , JobConf , Reporter)}.
 */

public class ZipSplit extends FileSplit
  {
  public static final Log LOG = LogFactory.getLog( "org.apache.hadoop.mapred.ZipSplit" );

  private String entryPath;

  ZipSplit()
    {
    super( null, 0, 0, null );
    }

  /**
   * Constructs a split from zip archive.
   *
   * @param file      the zip archive name
   * @param entryPath the path of the file to be read within the zip archive.
   * @param length    the uncompressed size of the file within the zip archive.
   * @param conf      see {@link JobConf}
   */
  public ZipSplit( Path file, String entryPath, long length, JobConf conf )
    {
    super( file, 0, length, conf );
    this.entryPath = entryPath;
    }

  /**
   * Constructor ZipSplit creates a new ZipSplit instance.
   *
   * @param file   of type Path
   * @param length of type long
   * @param conf   of type JobConf
   */
  public ZipSplit( Path file, long length, JobConf conf )
    {
    super( file, 0, length, conf );
    }

  /** The path of the file within the zip archive. */
  public String getEntryPath()
    {
    return entryPath;
    }

  // //////////////////////////////////////////
  // Writable methods
  // //////////////////////////////////////////
  public void write( DataOutput out ) throws IOException
    {
    super.write( out );
    UTF8.writeString( out, entryPath == null ? "" : entryPath );
    }

  public void readFields( DataInput in ) throws IOException
    {
    super.readFields( in );
    entryPath = UTF8.readString( in );
    }
  }
