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

package cascading.tap.hadoop;

import java.io.Closeable;
import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.util.Hadoop18TapUtil;
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
 *
 */
public class TapOutputCollector implements OutputCollector, Closeable
  {
  private static final Logger LOG = LoggerFactory.getLogger( TapOutputCollector.class );

  /** Field conf */
  private JobConf conf;
  /** Field writer */
  private RecordWriter writer;
  /** Field filenamePattern */
  private String filenamePattern = "%s%spart-%05d";
  /** Field filename */
  private String filename;
  /** Field tap */
  private Tap<FlowProcess<JobConf>, JobConf, RecordReader, OutputCollector> tap;
  /** Field prefix */
  private String prefix;
  /** Field isFileOutputFormat */
  private boolean isFileOutputFormat;
  /** Field reporter */
  private final Reporter reporter = Reporter.NULL;
  private final FlowProcess<JobConf> flowProcess;

  public TapOutputCollector( FlowProcess<JobConf> flowProcess, Tap<FlowProcess<JobConf>, JobConf, RecordReader, OutputCollector> tap, String prefix ) throws IOException
    {
    this.tap = tap;
    this.prefix = prefix == null || prefix.length() == 0 ? null : prefix;
    this.flowProcess = flowProcess;
    this.conf = this.flowProcess.getConfigCopy();
    this.filenamePattern = this.conf.get( "cascading.tapcollector.partname", this.filenamePattern );

    initialize();
    }

  protected void initialize() throws IOException
    {
    tap.sinkConfInit( flowProcess, conf );

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
    }

  }
