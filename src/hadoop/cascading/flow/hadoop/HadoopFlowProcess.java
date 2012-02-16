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

package cascading.flow.hadoop;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class HadoopFlowProcess is an implementation of {@link FlowProcess} for Hadoop. Use this interface to get direct
 * access to the Hadoop JobConf and Reporter interfaces.
 * <p/>
 * Be warned that coupling to this implementation will cause custom {@link cascading.operation.Operation}s to
 * fail if they are executed on a system other than Hadoop.
 *
 * @see cascading.flow.FlowSession
 * @see JobConf
 * @see Reporter
 */
public class HadoopFlowProcess extends FlowProcess<JobConf>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HadoopFlowProcess.class );

  /** Field jobConf */
  final JobConf jobConf;
  /** Field isMapper */
  private final boolean isMapper;
  /** Field reporter */
  Reporter reporter = Reporter.NULL;
  private OutputCollector outputCollector;
  private CompressionCodec codec;
  private String codecs;
  private String compress;

  public HadoopFlowProcess()
    {
    this.jobConf = new JobConf();
    this.isMapper = true;
    }

  public HadoopFlowProcess( JobConf jobConf )
    {
    this.jobConf = jobConf;
    this.isMapper = true;
    }

  public HadoopFlowProcess( FlowSession flowSession, JobConf jobConf )
    {
    super( flowSession );
    this.jobConf = jobConf;
    this.isMapper = true;
    }

  /**
   * Constructor HadoopFlowProcess creates a new HadoopFlowProcess instance.
   *
   * @param flowSession of type FlowSession
   * @param jobConf     of type JobConf
   */
  public HadoopFlowProcess( FlowSession flowSession, JobConf jobConf, boolean isMapper )
    {
    super( flowSession );
    this.jobConf = jobConf;
    this.isMapper = isMapper;
    }

  public HadoopFlowProcess( HadoopFlowProcess flowProcess, JobConf jobConf )
    {
    super( flowProcess.getCurrentSession() );
    this.jobConf = jobConf;
    this.isMapper = flowProcess.isMapper();
    this.reporter = flowProcess.getReporter();
    }

  @Override
  public FlowProcess copyWith( JobConf jobConf )
    {
    return new HadoopFlowProcess( this, jobConf );
    }

  /**
   * Method getJobConf returns the jobConf of this HadoopFlowProcess object.
   *
   * @return the jobConf (type JobConf) of this HadoopFlowProcess object.
   */
  public JobConf getJobConf()
    {
    return jobConf;
    }

  @Override
  public JobConf getConfigCopy()
    {
    return new JobConf( jobConf );
    }

  /**
   * Method isMapper returns true if this part of the FlowProcess is a MapReduce mapper. If false, it is a reducer.
   *
   * @return boolean
   */
  public boolean isMapper()
    {
    return isMapper;
    }

  public int getCurrentNumMappers()
    {
    return getJobConf().getNumMapTasks();
    }

  public int getCurrentNumReducers()
    {
    return getJobConf().getNumReduceTasks();
    }

  /**
   * Method getCurrentTaskNum returns the task number of this task. Task 0 is the first task.
   *
   * @return int
   */
  @Override
  public int getCurrentSliceNum()
    {
    return getJobConf().getInt( "mapred.task.partition", 0 );
    }

  @Override
  public int getNumProcessSlices()
    {
    if( isMapper() )
      return getCurrentNumMappers();
    else
      return getCurrentNumReducers();
    }

  /**
   * Method setReporter sets the reporter of this HadoopFlowProcess object.
   *
   * @param reporter the reporter of this HadoopFlowProcess object.
   */
  public void setReporter( Reporter reporter )
    {
    this.reporter = reporter;
    }

  /**
   * Method getReporter returns the reporter of this HadoopFlowProcess object.
   *
   * @return the reporter (type Reporter) of this HadoopFlowProcess object.
   */
  public Reporter getReporter()
    {
    return reporter;
    }

  private final Reporter getReporterOrFail()
    {
    if( reporter == null )
      throw new IllegalStateException( "unable to access the hadoop reporter, it is not available until the first map/reduce invocation" );

    return reporter;
    }

  public void setOutputCollector( OutputCollector outputCollector )
    {
    this.outputCollector = outputCollector;
    }

  public OutputCollector getOutputCollector()
    {
    return outputCollector;
    }

  @Override
  public Object getProperty( String key )
    {
    return jobConf.get( key );
    }

  @Override
  public Collection<String> getPropertyKeys()
    {
    Set<String> keys = new HashSet<String>();

    for( Map.Entry<String, String> entry : jobConf )
      keys.add( entry.getKey() );

    return Collections.unmodifiableSet( keys );
    }

  @Override
  public void keepAlive()
    {
    getReporterOrFail().progress();
    }

  @Override
  public void increment( Enum counter, long amount )
    {
    getReporterOrFail().incrCounter( counter, amount );
    }

  @Override
  public void increment( String group, String counter, long amount )
    {
    getReporterOrFail().incrCounter( group, counter, amount );
    }

  @Override
  public void setStatus( String status )
    {
    getReporterOrFail().setStatus( status );
    }

  @Override
  public boolean isCounterStatusInitialized()
    {
    return getReporter() != null;
    }

  @Override
  public TupleEntryIterator openTapForRead( Tap tap ) throws IOException
    {
    return tap.openForRead( this );
    }

  @Override
  public TupleEntryCollector openTapForWrite( Tap tap ) throws IOException
    {
    return tap.openForWrite( this, outputCollector );
    }

  @Override
  public TupleEntryCollector openTrapForWrite( Tap trap ) throws IOException
    {
    JobConf jobConf = new JobConf( getJobConf() );

    int stepNum = jobConf.getInt( "cascading.flow.step.num", 0 );
    String partname;

    if( jobConf.getBoolean( "mapred.task.is.map", true ) )
      partname = String.format( "-m-%05d-", stepNum );
    else
      partname = String.format( "-r-%05d-", stepNum );

    jobConf.set( "cascading.tapcollector.partname", "%s%spart" + partname + "%05d" );

    return trap.openForWrite( new HadoopFlowProcess( this, jobConf ), null );
    }

  @Override
  public TupleEntryCollector openSystemIntermediateForWrite() throws IOException
    {
    return new TupleEntryCollector( Fields.size( 2 ) )
    {
    @Override
    protected void collect( TupleEntry tupleEntry )
      {
      try
        {
        getOutputCollector().collect( tupleEntry.get( 0 ), tupleEntry.get( 1 ) );
        }
      catch( IOException exception )
        {
        throw new CascadingException( "failed collecting key and value", exception );
        }
      }
    };
    }

  @Override
  public JobConf copyConfig( JobConf jobConf )
    {
    return new JobConf( jobConf );
    }

  @Override
  public Map<String, String> diffConfigIntoMap( JobConf defaultConfig, JobConf updatedConfig )
    {
    return HadoopUtil.getConfig( defaultConfig, updatedConfig );
    }

  @Override
  public JobConf mergeMapIntoConfig( JobConf defaultConfig, Map<String, String> map )
    {
    return HadoopUtil.mergeConf( defaultConfig, map, false );
    }
  }
