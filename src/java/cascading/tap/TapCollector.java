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

package cascading.tap;

import java.io.IOException;

import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.Tuples;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/** Class TapCollector is a kind of {@link TupleCollector} that writes to a particular {@link Tap} instance. */
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

    initalize();
    }

  private void initalize() throws IOException
    {
    tap.sinkInit( conf );
    tap.makeDirs( conf ); // required

    OutputFormat outputFormat = conf.getOutputFormat();

    if( outputFormat instanceof JobConfigurable )
      ( (JobConfigurable) outputFormat ).configure( conf );

    filename = String.format( filenamePattern, conf.getInt( "mapred.task.partition", 0 ) );
    writer = outputFormat.getRecordWriter( null, conf, filename, Reporter.NULL );
    }

  protected void collect( Tuple tuple )
    {
    try
      {
      writer.write( Tuples.NULL, tuple );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to write to: " + filename, exception );
      }
    }

  @Override
  public void close() throws IOException
    {
    try
      {
      writer.close( Reporter.NULL );
      }
    catch( IOException exception )
      {
      LOG.warn( "exception closing: " + filename, exception );
      throw exception;
      }
    }

  /**
   * Method collect writes the given values to the {@link Tap} this instance encapsulates.
   *
   * @param writableComparable of type WritableComparable
   * @param writable           of type Writable
   * @throws IOException when
   */
  public void collect( WritableComparable writableComparable, Writable writable ) throws IOException
    {
    writer.write( writableComparable, writable );
    }
  }
