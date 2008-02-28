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
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * An {@link InputFormat} for zip files. Each file within a zip file is broken
 * into lines.Either line-feed or carriage-return are used to signal end of
 * line. Keys are the position in the file, and values are the line of text.
 */
public class ZipInputFormat extends FileInputFormat<LongWritable, Text> implements JobConfigurable
  {
  public void configure( JobConf conf )
    {

    }

  /**
   * Return true only if the file is in ZIP format.
   *
   * @param fs   the file system that the file is on
   * @param file the path that represents this file
   * @return is this file splitable?
   */
  protected boolean isSplitable( FileSystem fs, Path file )
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "verifying ZIP format for file: " + file.toString() );

    boolean splitable = true;
    ZipInputStream zipInputStream = null;

    try
      {
      zipInputStream = new ZipInputStream( fs.open( file ) );
      ZipEntry zipEntry = zipInputStream.getNextEntry();

      if( zipEntry == null )
        throw new IOException( "no entries found, empty zip file" );

      if( LOG.isDebugEnabled() )
        LOG.debug( "ZIP format verification successful" );
      }
    catch( IOException exception )
      {
      LOG.error( "exception encountered while trying to open and read ZIP input stream", exception );
      splitable = false;
      }
    finally
      {
      try
        {
        if( zipInputStream != null )
          zipInputStream.close();
        }
      catch( IOException exception )
        {
        LOG.error( "exception while trying to close ZIP input stream", exception );
        }
      }

    return splitable;
    }

  /**
   * Splits files returned by {@link #listPaths(JobConf)}. Each file is
   * expected to be in zip format and each split corresponds to
   * {@link ZipEntry}.
   *
   * @param job       the JobConf data structure, see {@link JobConf}
   * @param numSplits the number of splits required. Ignored here
   * @throws IOException if input files are not in zip format
   */
  public InputSplit[] getSplits( JobConf job, int numSplits ) throws IOException
    {

    if( LOG.isDebugEnabled() )
      LOG.debug( "start splitting input ZIP files" );

    Path[] files = listPaths( job );

    for( int i = 0; i < files.length; i++ )
      { // check we have valid files
      Path file = files[ i ];
      FileSystem fs = file.getFileSystem( job );

      if( fs.isDirectory( file ) || !fs.exists( file ) )
        throw new IOException( "not a file: " + files[ i ] );
      }

    // generate splits
    ArrayList<ZipSplit> splits = new ArrayList<ZipSplit>( numSplits );
    ZipInputStream zipInputStream = null;
    ZipEntry zipEntry = null;

    for( int i = 0; i < files.length; i++ )
      {
      Path file = files[ i ];
      FileSystem fs = file.getFileSystem( job );

      if( LOG.isDebugEnabled() )
        LOG.debug( "opening zip file: " + file.toString() );

      try
        {
        zipInputStream = new ZipInputStream( fs.open( file ) );

        while( ( zipEntry = zipInputStream.getNextEntry() ) != null )
          {
          ZipSplit zipSplit = new ZipSplit( file, zipEntry.getName(), zipEntry.getSize(), job );

          if( LOG.isDebugEnabled() )
            LOG.debug( String.format( "creating split for zip entry: %s size: %d method: %s compressed size: %d", zipEntry.getName(), zipEntry.getSize(),
              ZipEntry.DEFLATED == zipEntry.getMethod() ? "DEFLATED" : "STORED", zipEntry.getCompressedSize() ) );

          splits.add( zipSplit );
          }
        }
      finally
        {
        if( zipInputStream != null )
          zipInputStream.close();
        }

      }

    if( LOG.isDebugEnabled() )
      LOG.debug( "end splitting input ZIP files" );

    return splits.toArray( new ZipSplit[splits.size()] );
    }

  public RecordReader<LongWritable, Text> getRecordReader( InputSplit genericSplit, JobConf job, Reporter reporter ) throws IOException
    {
    reporter.setStatus( genericSplit.toString() );

    ZipSplit split = (ZipSplit) genericSplit;
    Path file = split.getFile();
    String entryPath = split.getEntryPath();
    long length = split.getLength();

    // Set it max value if length is unknown.
    // Setting length to Max value does not have
    // a side effect as Record reader would not be
    // able to read past the actual size of
    // current entry.
    length = length == -1 ? Long.MAX_VALUE - 1 : length;

    FileSystem fs = file.getFileSystem( job );
    ZipInputStream zipInputStream = new ZipInputStream( fs.open( file ) );
    ZipEntry zipEntry = zipInputStream.getNextEntry();

    while( zipEntry != null && !zipEntry.getName().equals( entryPath ) )
      zipEntry = zipInputStream.getNextEntry();

    return new LineRecordReader( zipInputStream, 0, length );
    }
  }