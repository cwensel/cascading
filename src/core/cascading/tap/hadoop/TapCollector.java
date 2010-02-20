/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * Class TapCollector is a kind of {@link cascading.tuple.TupleEntryCollector} that writes tuples to the resource managed by
 * a particular {@link cascading.tap.Tap} instance.
 */
public class TapCollector extends TupleEntryCollector implements OutputCollector
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( TapCollector.class );

  /** Field conf */
  private JobConf conf;
  /** Field writer */
  private RecordWriter writer;
  /** Field filenamePattern */
  private String filenamePattern = "%s%spart-%05d";
  /** Field filename */
  private String filename;
  /** Field tap */
  private Tap tap;
  /** Field prefix */
  private String prefix;
  /** Field outputEntry */
  private TupleEntry outputEntry;
  /** Field isFileOutputFormat */
  private boolean isFileOutputFormat;
  /** Field reporter */
  private Reporter reporter = Reporter.NULL;

  /**
   * Constructor TapCollector creates a new TapCollector instance.
   *
   * @param tap  of type Tap
   * @param conf of type JobConf
   * @throws IOException when fails to initialize
   */
  public TapCollector( Tap tap, JobConf conf ) throws IOException
    {
    this( tap, null, conf );
    }

  /**
   * Constructor TapCollector creates a new TapCollector instance.
   *
   * @param tap    of type Tap
   * @param prefix of type String
   * @param conf   of type JobConf
   * @throws IOException when fails to initialize
   */
  public TapCollector( Tap tap, String prefix, JobConf conf ) throws IOException
    {
    this.tap = tap;
    this.prefix = prefix == null || prefix.length() == 0 ? null : prefix;
    this.conf = new JobConf( conf );
    this.outputEntry = new TupleEntry( tap.getSinkFields() );
    this.filenamePattern = conf.get( "cascading.tapcollector.partname", this.filenamePattern );

    initalize();
    }

  private void initalize() throws IOException
    {
    tap.sinkInit( conf ); // tap should not delete if called within a task

    OutputFormat outputFormat = conf.getOutputFormat();

    isFileOutputFormat = outputFormat instanceof FileOutputFormat;

    if( isFileOutputFormat )
      {
      Hadoop18TapUtil.setupJob( conf );

      if( prefix != null )
        filename = String.format( filenamePattern, prefix, "/", conf.getInt( "mapred.task.partition", 0 ) );
      else
        filename = String.format( filenamePattern, "", "", conf.getInt( "mapred.task.partition", 0 ) );

      Hadoop18TapUtil.setupTask( conf );
      }

    writer = outputFormat.getRecordWriter( null, conf, filename, Reporter.NULL );
    }

  public void setReporter( Reporter reporter )
    {
    this.reporter = reporter;
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

  @Override
  public void close()
    {
    try
      {
      if( isFileOutputFormat )
        LOG.info( "closing tap collector for: " + new Path( tap.getPath(), filename ) );
      else
        LOG.info( "closing tap collector for: " + tap.toString() );

      try
        {
        writer.close( reporter );
        }
      finally
        {
        if( isFileOutputFormat )
          {
          if( Hadoop18TapUtil.needsTaskCommit( conf ) )
            Hadoop18TapUtil.commitTask( conf );

          Hadoop18TapUtil.cleanupJob( conf );
          }
        }
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
    reporter.progress();
    writer.write( writableComparable, writable );
    }
  }
