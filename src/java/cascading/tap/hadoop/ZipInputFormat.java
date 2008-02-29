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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
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
 * Class ZipInputFormat ia an {@link InputFormat} for zip files. Each file within a zip file is broken
 * into lines. Either line-feed or carriage-return are used to signal end of
 * line. Keys are the position in the file, and values are the line of text.
 * <p/>
 * If the underlying {@link FileSystem} is HDFS or FILE, each {@link ZipEntry} is returned
 * as a unique split. Otherwise this input format returns false for isSplitable, and will
 * subsequently iterate over each ZipEntry and treat all internal files as the 'same' file.
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
    if( !isAllowSplits( fs ) )
      return false;

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
      safeClose( zipInputStream );
      }

    return splitable;
    }

  @Override
  protected Path[] listPaths( JobConf jobConf ) throws IOException
    {
    Path[] dirs = jobConf.getInputPaths();

    if( dirs.length == 0 )
      throw new IOException( "no input paths specified in job" );

    for( Path dir : dirs )
      {
      FileSystem fs = dir.getFileSystem( jobConf );

      if( !fs.isFile( dir ) )
        throw new IOException( "does not support directories: " + dir );
      }

    return dirs;
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

      if( !fs.isFile( file ) || !fs.exists( file ) )
        throw new IOException( "not a file: " + files[ i ] );
      }

    // generate splits
    ArrayList<ZipSplit> splits = new ArrayList<ZipSplit>( numSplits );

    for( int i = 0; i < files.length; i++ )
      {
      Path file = files[ i ];
      FileSystem fs = file.getFileSystem( job );

      if( LOG.isDebugEnabled() )
        LOG.debug( "opening zip file: " + file.toString() );

      if( isAllowSplits( fs ) )
        makeSplits( job, splits, fs, file );
      else
        makeSplit( job, splits, file );
      }

    if( LOG.isDebugEnabled() )
      LOG.debug( "end splitting input ZIP files" );

    return splits.toArray( new ZipSplit[splits.size()] );
    }

  private void makeSplit( JobConf job, ArrayList<ZipSplit> splits, Path file ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "creating split for zip: " + file );

    // unknown uncompressed size. if set to compressed size, data will be truncated
    splits.add( new ZipSplit( file, -1, job ) );
    }

  private void makeSplits( JobConf job, ArrayList<ZipSplit> splits, FileSystem fs, Path file ) throws IOException
    {
    ZipInputStream zipInputStream = new ZipInputStream( fs.open( file ) );

    try
      {
      ZipEntry zipEntry;

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
      safeClose( zipInputStream );
      }
    }

  public RecordReader<LongWritable, Text> getRecordReader( InputSplit genericSplit, JobConf job, Reporter reporter ) throws IOException
    {
    reporter.setStatus( genericSplit.toString() );

    ZipSplit split = (ZipSplit) genericSplit;
    Path file = split.getPath();
    long length = split.getLength();

    // Set it max value if length is unknown.
    // Setting length to Max value does not have
    // a side effect as Record reader would not be
    // able to read past the actual size of
    // current entry.
    length = length == -1 ? Long.MAX_VALUE - 1 : length;

    FileSystem fs = file.getFileSystem( job );

    FSDataInputStream inputStream = fs.open( file );

    if( isAllowSplits( fs ) )
      return getReaderForEntry( inputStream, split, length );
    else
      return getReaderForAll( inputStream, fs.getFileStatus( file ).getLen() ); // hopefully an efficient call
    }

  private RecordReader<LongWritable, Text> getReaderForAll( final FSDataInputStream inputStream, final long compressedLength ) throws IOException
    {
    Enumeration<InputStream> enumeration = new Enumeration<InputStream>()
    {
    boolean returnCurrent = false;
    ZipEntry nextEntry;
    ZipInputStream zipInputStream = new ZipInputStream( inputStream );
    InputStream closeableInputStream = makeInputStream( zipInputStream );

    public boolean hasMoreElements()
      {
      if( returnCurrent )
        return nextEntry != null;

      getNext();

      return nextEntry != null;
      }

    public InputStream nextElement()
      {
      if( returnCurrent )
        {
        returnCurrent = false;
        return closeableInputStream;
        }

      getNext();

      if( nextEntry == null )
        throw new IllegalStateException( "no more zip entries in zip input stream" );

      return closeableInputStream;
      }

    private void getNext()
      {
      try
        {
        nextEntry = zipInputStream.getNextEntry();

        while( nextEntry != null && nextEntry.isDirectory() )
          nextEntry = zipInputStream.getNextEntry();

        returnCurrent = true;
        }
      catch( IOException exception )
        {
        throw new RuntimeException( "could not get next zip entry", exception );
        }
      finally
        {
        // i think, better than sending across a fake input stream that closes the zip
        if( nextEntry == null )
          safeClose( zipInputStream );
        }
      }

    private InputStream makeInputStream( ZipInputStream zipInputStream )
      {
      return new FilterInputStream( zipInputStream )
      {
      @Override
      public void close() throws IOException
        {
        // do nothing
        }
      };
      }
    };

    return new LineRecordReader( new SequenceInputStream( enumeration ), 0, Long.MAX_VALUE )
    {
    @Override
    public float getProgress()
      {
      if( 0 == compressedLength )
        return 0.0f;
      else
        return Math.min( 1.0f, getPosSafe( inputStream ) / (float) compressedLength );
      }
    };
    }

  private long getPosSafe( FSDataInputStream inputStream )
    {
    try
      {
      return inputStream.getPos();
      }
    catch( IOException exception )
      {
      LOG.warn( "could not get pos from FSDataInputStream" );
      }

    return 0;
    }

  private RecordReader<LongWritable, Text> getReaderForEntry( FSDataInputStream inputStream, ZipSplit split, long length ) throws IOException
    {
    ZipInputStream zipInputStream = new ZipInputStream( inputStream );
    String entryPath = split.getEntryPath();

    ZipEntry zipEntry = zipInputStream.getNextEntry();

    while( zipEntry != null && !zipEntry.getName().equals( entryPath ) )
      zipEntry = zipInputStream.getNextEntry();

    return new LineRecordReader( zipInputStream, 0, length );
    }

  protected boolean isAllowSplits( FileSystem fs )
    {
    // only allow if fs is local or dfs
    URI uri = fs.getUri();
    String scheme = uri.getScheme();

    return scheme.equalsIgnoreCase( "hdfs" ) || scheme.equalsIgnoreCase( "file" );
    }

  private void safeClose( ZipInputStream zipInputStream )
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

  }