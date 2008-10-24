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

import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * Class TapCollector is a kind of {@link TupleCollector} that writes tuples to the resource managed by
 * a particular {@link cascading.tap.Tap} instance.
 */
public class TapCollector extends TupleCollector implements OutputCollector
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( TapCollector.class );

  /** Field conf */
  private JobConf conf;
  /** Field writer */
  private RecordWriter writer;
  /** Field filenamePattern */
  private String filenamePattern = "part-%05d";
  /** Field filename */
  private String filename;
  /** Field tap */
  private Tap tap;
  /** Field outputEntry */
  private TupleEntry outputEntry;
  private OutputCommitter outputCommitter;
  private JobContext jobContext;

  /**
   * Constructor TapCollector creates a new TapCollector instance.
   *
   * @param tap  of type Tap
   * @param conf of type JobConf
   * @throws IOException when
   */
  public TapCollector( Tap tap, JobConf conf ) throws IOException
    {
    this.tap = tap;
    this.conf = new JobConf( conf );
    this.outputEntry = new TupleEntry( tap.getSinkFields() );

    initalize();
    }

  private void initalize() throws IOException
    {
    tap.sinkInit( conf ); // tap should not delete if called within a task

    if( !tap.makeDirs( conf ) ) // required
      throw new TapException( "unable to make dirs for: " + tap.toString() );


    OutputFormat outputFormat = conf.getOutputFormat();

    Path outputPath = FileOutputFormat.getOutputPath( conf );
    FileSystem fileSystem = FileSystem.get( outputPath.toUri(), conf );

    filename = String.format( filenamePattern, conf.getInt( "mapred.task.partition", 0 ) );

    conf.set( "mapred.work.output.dir", outputPath.toString() );

    if( conf.getOutputCommitter() instanceof FileOutputCommitter ) // only file based writing uses temp dirs
      fileSystem.mkdirs( new Path( conf.get( "mapred.work.output.dir" ), FileOutputCommitter.TEMP_DIR_NAME ) );

    writer = outputFormat.getRecordWriter( null, conf, filename, Reporter.NULL );
    }

  protected void collect( Tuple tuple )
    {
    try
      {
      outputEntry.setTuple( tuple );

      tap.sink( outputEntry, this );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to write to: " + filename, exception );
      }
    }

  private void moveTaskOutputs() throws IOException
    {
    Path outputPath = FileOutputFormat.getOutputPath( conf );
    Path taskPath = FileOutputFormat.getTaskOutputPath( conf, "_unused_" ).getParent();

    FileSystem fileSystem = FileSystem.get( outputPath.toUri(), conf );

    if( !fileSystem.getFileStatus( taskPath ).isDir() )
      throw new IOException( "path is not a directory: " + taskPath );

    FileStatus[] statuses = fileSystem.listStatus( taskPath );

    for( FileStatus status : statuses )
      {
      Path sourcePath = status.getPath();

      if( status.isDir() )
        throw new IOException( "path is a directory, no support for nested directories: " + sourcePath );

      Path targetPath = new Path( outputPath, sourcePath.getName() );

      fileSystem.rename( sourcePath, targetPath );

      LOG.debug( "moved " + sourcePath + " to " + targetPath );
      }

    // remove _temporary directory
    fileSystem.delete( new Path( conf.get( "mapred.work.output.dir" ), FileOutputCommitter.TEMP_DIR_NAME ), true );
    }

  @Override
  public void close()
    {
    try
      {
      writer.close( Reporter.NULL );

      if( conf.getOutputCommitter() instanceof FileOutputCommitter )
        moveTaskOutputs();

      }
    catch( IOException exception )
      {
      LOG.warn( "exception closing: " + filename, exception );
      throw new TapException( "exception closing: " + filename, exception );
      }
    }

  /**
   * Method collect writes the given values to the {@link Tap} this instance encapsulates.
   *
   * @param writableComparable of type WritableComparable
   * @param writable           of type Writable
   * @throws IOException when
   */
  public void collect( Object writableComparable, Object writable ) throws IOException
    {
    writer.write( writableComparable, writable );
    }
  }
