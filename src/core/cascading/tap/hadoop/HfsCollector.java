/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.Flow;
import cascading.flow.FlowContext;
import cascading.flow.FlowSession;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

/**
 * Class TapCollector is a kind of {@link cascading.tuple.TupleEntryCollector} that writes tuples to the resource managed by
 * a particular {@link cascading.tap.Tap} instance.
 */
public class HfsCollector extends HadoopEntryCollector
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( HfsCollector.class );

  /** Field conf */
  private Job job;
  /** Field writer */
  private RecordWriter writer;
  /** Field filenamePattern */
//  private String filenamePattern = "%s%spart-%05d";
  /** Field filename */
  //  private String filename;
  /** Field tap */
  private Tap tap;
  /** Field prefix */
  private String prefix;
  /** Field outputEntry */
  private TupleEntry outputEntry;
  /** Field isFileOutputFormat */
  private boolean isFileOutputFormat;
  /** Field taskAttemptContext */
  private TaskAttemptContext taskAttemptContext;
  /** Field outputCommitter */
  private OutputCommitter outputCommitter;

  /**
   * Constructor TapCollector creates a new TapCollector instance.
   *
   * @param tap         of type Tap
   * @param flowContext of type JobConf
   * @throws IOException when fails to initialize
   */
  public HfsCollector( Hfs tap, FlowContext<Configuration> flowContext ) throws IOException
    {
    this( tap, null, flowContext.getConfiguration() );
    }

  /**
   * Constructor TapCollector creates a new TapCollector instance.
   *
   * @param tap    of type Tap
   * @param prefix of type String
   * @param conf   of type JobConf
   * @throws IOException when fails to initialize
   */
  public HfsCollector( Hfs tap, String prefix, Configuration conf ) throws IOException
    {
    this.tap = tap;
    this.prefix = prefix == null || prefix.length() == 0 ? null : prefix;
    this.job = new Job( conf );
    this.outputEntry = new TupleEntry( tap.getSinkFields() );
//    this.filenamePattern = conf.get( "cascading.tapcollector.partname", this.filenamePattern );

    initalize();
    }

  private void initalize() throws IOException
    {
    tap.sinkInit( job ); // tap should not delete if called within a task

    OutputFormat outputFormat = null;

    try
      {
      outputFormat = ReflectionUtils.newInstance( job.getOutputFormatClass(), job.getConfiguration() );
      }
    catch( ClassNotFoundException exception )
      {
      throw new RuntimeException( exception );
      }

    isFileOutputFormat = outputFormat instanceof FileOutputFormat;

//    if( isFileOutputFormat )
//      {
//      if( prefix != null )
//        filename = String.format( filenamePattern, prefix, "/", job.getConfiguration().getInt( "mapred.task.partition", 0 ) );
//      else
//        filename = String.format( filenamePattern, "", "", job.getConfiguration().getInt( "mapred.task.partition", 0 ) );
//      }

    try
      {
      taskAttemptContext = Hadoop21TapUtil.getAttemptContext( job.getConfiguration() );

      outputCommitter = outputFormat.getOutputCommitter( taskAttemptContext );
      outputCommitter.setupJob( taskAttemptContext );
      outputCommitter.setupTask( taskAttemptContext );

      writer = outputFormat.getRecordWriter( taskAttemptContext );

      TaskAttemptID taskId = Hadoop21TapUtil.getTaskAttemptId( job.getConfiguration() );
      TaskInputOutputContext taskContext = new HfsContext( job.getConfiguration(), taskId, writer, null, null );
      hadoopFlowProcess = new HadoopFlowProcess( new FlowSession(), taskContext, true );
      }
    catch( InterruptedException exception )
      {
      throw new RuntimeException( exception );
      }
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
      throw new TapException( "unable to write to: " + tap.toString(), exception );
      }
    }

  @Override
  public void close()
    {
    try
      {
//      if( isFileOutputFormat )
//        LOG.info( "closing tap collector for: " + new Path( tap.getPath(), filename ) );
//      else
      LOG.info( "closing tap collector for: " + tap.toString() );

      try
        {
        writer.close( taskAttemptContext );
        }
      catch( InterruptedException exception )
        {
        throw new RuntimeException( exception );
        }
      finally
        {
        if( outputCommitter.needsTaskCommit( taskAttemptContext ) )
          outputCommitter.commitTask( taskAttemptContext );

        if( !Flow.isInflow( taskAttemptContext.getConfiguration() ) )
          outputCommitter.cleanupJob( taskAttemptContext );
        }
      }
    catch( IOException exception )
      {
      LOG.warn( "exception closing: " + tap, exception );
      throw new TapException( "exception closing: " + tap, exception );
      }
    }

  }
