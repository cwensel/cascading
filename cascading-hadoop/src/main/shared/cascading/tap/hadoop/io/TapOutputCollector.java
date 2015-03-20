/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.io.Closeable;
import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.MapRed;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.util.Hadoop18TapUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.hadoop.util.HadoopUtil.asJobConfInstance;

/**
 *
 */
public class TapOutputCollector implements OutputCollector, Closeable
  {
  private static final Logger LOG = LoggerFactory.getLogger( TapOutputCollector.class );

  public static final String PART_TASK_PATTERN = "%s%spart-%05d";
  public static final String PART_TASK_SEQ_PATTERN = "%s%spart-%05d-%05d";

  /** Field conf */
  private Configuration conf;
  /** Field writer */
  private RecordWriter writer;
  /** Field filenamePattern */
  private String filenamePattern;
  /** Field filename */
  private String filename;
  /** Field tap */
  private Tap<Configuration, RecordReader, OutputCollector> tap;
  /** Field prefix */
  private String prefix;
  /** Field sequence */
  private long sequence;
  /** Field isFileOutputFormat */
  private boolean isFileOutputFormat;
  private final FlowProcess<? extends Configuration> flowProcess;

  public TapOutputCollector( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap ) throws IOException
    {
    this( flowProcess, tap, null );
    }

  public TapOutputCollector( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, String prefix ) throws IOException
    {
    this( flowProcess, tap, prefix, -1 );
    }

  public TapOutputCollector( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, String prefix, long sequence ) throws IOException
    {
    this.tap = tap;
    this.sequence = sequence;
    this.prefix = prefix == null || prefix.length() == 0 ? null : prefix;
    this.flowProcess = flowProcess;
    this.conf = this.flowProcess.getConfigCopy();
    this.filenamePattern = this.conf.get( "cascading.tapcollector.partname", sequence == -1 ? PART_TASK_PATTERN : PART_TASK_SEQ_PATTERN );

    initialize();
    }

  protected void initialize() throws IOException
    {
    tap.sinkConfInit( flowProcess, conf );

    OutputFormat outputFormat = asJobConfInstance( conf ).getOutputFormat();

    // todo: use OutputCommitter class

    isFileOutputFormat = outputFormat instanceof FileOutputFormat;

    if( isFileOutputFormat )
      {
      Hadoop18TapUtil.setupJob( conf );
      Hadoop18TapUtil.setupTask( conf );

      if( prefix != null )
        filename = String.format( filenamePattern, prefix, "/", conf.getInt( "mapred.task.partition", 0 ), sequence );
      else
        filename = String.format( filenamePattern, "", "", conf.getInt( "mapred.task.partition", 0 ), sequence );
      }

    LOG.info( "creating path: {}", filename );

    writer = outputFormat.getRecordWriter( null, asJobConfInstance( conf ), filename, getReporter() );
    }

  private Reporter getReporter()
    {
    Reporter reporter = Reporter.NULL;

    if( flowProcess instanceof MapRed )
      reporter = ( (MapRed) flowProcess ).getReporter(); // may return Reporter.NULL

    return reporter;
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
    flowProcess.keepAlive();
    writer.write( writableComparable, writable );
    }

  public void close()
    {
    try
      {
      if( isFileOutputFormat )
        LOG.info( "closing tap collector for: {}", new Path( tap.getIdentifier(), filename ) );
      else
        LOG.info( "closing tap collector for: {}", tap );

      try
        {
        writer.close( getReporter() );
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
    }
  }
