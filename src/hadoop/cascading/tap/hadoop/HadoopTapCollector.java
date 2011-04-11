/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntrySchemeCollector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class TapCollector is a kind of {@link cascading.tuple.TupleEntryCollector} that writes tuples to the resource managed by
 * a particular {@link cascading.tap.Tap} instance.
 */
public class HadoopTapCollector extends TupleEntrySchemeCollector implements OutputCollector
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HadoopTapCollector.class );

  /** Field conf */
  private final JobConf conf;
  /** Field writer */
  private RecordWriter writer;
  /** Field filenamePattern */
  private String filenamePattern = "%s%spart-%05d";
  /** Field filename */
  private String filename;
  /** Field flowProcess */
  private final HadoopFlowProcess hadoopFlowProcess;
  /** Field tap */
  private final Tap<HadoopFlowProcess, JobConf, RecordReader, OutputCollector> tap;
  /** Field prefix */
  private final String prefix;
  /** Field isFileOutputFormat */
  private boolean isFileOutputFormat;
  /** Field reporter */
  private final Reporter reporter = Reporter.NULL;

  /**
   * Constructor TapCollector creates a new TapCollector instance.
   *
   * @param hadoopFlowProcess
   * @param tap               of type Tap  @throws IOException when fails to initialize
   */
  public HadoopTapCollector( HadoopFlowProcess hadoopFlowProcess, Tap<HadoopFlowProcess, JobConf, RecordReader, OutputCollector> tap ) throws IOException
    {
    this( hadoopFlowProcess, tap, null );
    }

  /**
   * Constructor TapCollector creates a new TapCollector instance.
   *
   * @param hadoopFlowProcess
   * @param tap               of type Tap
   * @param prefix            of type String
   * @throws IOException when fails to initialize
   */
  public HadoopTapCollector( HadoopFlowProcess hadoopFlowProcess, Tap<HadoopFlowProcess, JobConf, RecordReader, OutputCollector> tap, String prefix ) throws IOException
    {
    super( hadoopFlowProcess, tap.getScheme() );
    this.hadoopFlowProcess = hadoopFlowProcess;

    this.tap = tap;
    this.prefix = prefix == null || prefix.length() == 0 ? null : prefix;
    this.conf = new JobConf( hadoopFlowProcess.getJobConf() );
    this.filenamePattern = conf.get( "cascading.tapcollector.partname", this.filenamePattern );

    this.setOutput( this );

    initialize();
    }

  private void initialize() throws IOException
    {
    tap.sinkConfInit( null, conf ); // tap should not delete if called within a task

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

    sinkCall.setOutput( this );
    }

  @Override
  public void close()
    {
    try
      {
      if( isFileOutputFormat )
        LOG.info( "closing tap collector for: {}", new Path( tap.getPath(), filename ) );
      else
        LOG.info( "closing tap collector for: {}", tap );

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
      LOG.warn( "exception closing: {}", filename, exception );
      throw new TapException( "exception closing: " + filename, exception );
      }
    finally
      {
      super.close();
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
    hadoopFlowProcess.getReporter().progress();
    writer.write( writableComparable, writable );
    }
  }
