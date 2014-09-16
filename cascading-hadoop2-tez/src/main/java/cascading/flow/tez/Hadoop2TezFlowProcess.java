/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.tez;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Writer;

/**
 * Class HadoopFlowProcess is an implementation of {@link cascading.flow.FlowProcess} for Hadoop. Use this interface to get direct
 * access to the Hadoop JobConf and Reporter interfaces.
 * <p/>
 * Be warned that coupling to this implementation will cause custom {@link cascading.operation.Operation}s to
 * fail if they are executed on a system other than Hadoop.
 *
 * @see cascading.flow.FlowSession
 */
public class Hadoop2TezFlowProcess extends FlowProcess<TezConfiguration>
  {
  /** Field jobConf */
  final TezConfiguration configuration;
  private ProcessorContext context;
  private Writer writer;

  public Hadoop2TezFlowProcess()
    {
    this.configuration = new TezConfiguration();
    }

  public Hadoop2TezFlowProcess( TezConfiguration configuration )
    {
    this.configuration = configuration;
    }

  public Hadoop2TezFlowProcess( FlowSession flowSession, ProcessorContext context, TezConfiguration configuration )
    {
    super( flowSession );
    this.context = context;
    this.configuration = configuration;
    }

  public Hadoop2TezFlowProcess( Hadoop2TezFlowProcess flowProcess, TezConfiguration configuration )
    {
    super( flowProcess.getCurrentSession() );
    this.context = flowProcess.context;
    this.configuration = configuration;
    }

  public ProcessorContext getContext()
    {
    return context;
    }

  public void setWriter( Writer writer )
    {
    this.writer = writer;
    }

  @Override
  public FlowProcess copyWith( TezConfiguration configuration )
    {
    return new Hadoop2TezFlowProcess( this, configuration );
    }

  /**
   * Method getJobConf returns the jobConf of this HadoopFlowProcess object.
   *
   * @return the jobConf (type JobConf) of this HadoopFlowProcess object.
   */
  public TezConfiguration getConfiguration()
    {
    return configuration;
    }

  @Override
  public TezConfiguration getConfigCopy()
    {
    return new TezConfiguration( configuration );
    }

  /**
   * Method getCurrentTaskNum returns the task number of this task. Task 0 is the first task.
   *
   * @return int
   */
  @Override
  public int getCurrentSliceNum()
    {
    return getConfiguration().getInt( "mapred.task.partition", 0 ); // TODO: likely incorrect
    }

  @Override
  public int getNumProcessSlices()
    {
    return 0;

//    if( isMapper() )
//      return getCurrentNumMappers();
//    else
//      return getCurrentNumReducers();
    }

  /**
   * Method setReporter sets the reporter of this HadoopFlowProcess object.
   *
   * @param reporter the reporter of this HadoopFlowProcess object.
   */
//  public void setReporter( Reporter reporter )
//    {
//    this.reporter = reporter;
//    }

  /**
   * Method getReporter returns the reporter of this HadoopFlowProcess object.
   *
   * @return the reporter (type Reporter) of this HadoopFlowProcess object.
   */
//  public Reporter getReporter()
//    {
//    return reporter;
//    }

//  public void setOutputCollector( OutputCollector outputCollector )
//    {
//    this.outputCollector = outputCollector;
//    }

//  public OutputCollector getOutputCollector()
//    {
//    return outputCollector;
//    }
  @Override
  public Object getProperty( String key )
    {
    return configuration.get( key );
    }

  @Override
  public Collection<String> getPropertyKeys()
    {
    Set<String> keys = new HashSet<String>();

    for( Map.Entry<String, String> entry : configuration )
      keys.add( entry.getKey() );

    return Collections.unmodifiableSet( keys );
    }

  @Override
  public Object newInstance( String className )
    {
    if( className == null || className.isEmpty() )
      return null;

    try
      {
      Class type = (Class) Hadoop2TezFlowProcess.class.getClassLoader().loadClass( className.toString() );

      return ReflectionUtils.newInstance( type, configuration );
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( "unable to load class: " + className.toString(), exception );
      }
    }

  @Override
  public void keepAlive()
    {
//    context.
    }

  @Override
  public void increment( Enum counter, long amount )
    {
    if( context != null )
      context.getCounters().findCounter( counter ).increment( amount );
    }

  @Override
  public void increment( String group, String counter, long amount )
    {
    if( context != null )
      context.getCounters().findCounter( group, counter ).increment( amount );
    }

  @Override
  public void setStatus( String status )
    {
//    getReporter().setStatus( status );
    }

  @Override
  public boolean isCounterStatusInitialized()
    {
//    return getReporter() != null;
    return false;
    }

  @Override
  public TupleEntryIterator openTapForRead( Tap tap ) throws IOException
    {
    return tap.openForRead( this );
    }

  @Override
  public TupleEntryCollector openTapForWrite( Tap tap ) throws IOException
    {
    return tap.openForWrite( this, null ); // do not honor sinkmode as this may be opened across tasks
    }

  @Override
  public TupleEntryCollector openTrapForWrite( Tap trap ) throws IOException
    {
    TezConfiguration jobConf = new TezConfiguration( getConfiguration() );

    int stepNum = jobConf.getInt( "cascading.flow.step.num", 0 );
    int nodeNum = jobConf.getInt( "cascading.flow.node.num", 0 );

    String partname = String.format( "-%05d-%05d-", stepNum, nodeNum );

    jobConf.set( "cascading.tapcollector.partname", "%s%spart" + partname + "%05d" );

    return trap.openForWrite( new Hadoop2TezFlowProcess( this, jobConf ), null ); // do not honor sinkmode as this may be opened across tasks
    }

  @Override
  public TupleEntryCollector openSystemIntermediateForWrite() throws IOException
    {
    return null;
/*
    return new TupleEntryCollector( Fields.size( 2 ) )
    {
    @Override
    protected void collect( TupleEntry tupleEntry )
      {
      try
        {
        getOutputCollector().collect( tupleEntry.getObject( 0 ), tupleEntry.getObject( 1 ) );
        }
      catch( IOException exception )
        {
        throw new CascadingException( "failed collecting key and value", exception );
        }
      }
    };
*/
    }

  @Override
  public <C> C copyConfig( C config )
    {
    return HadoopUtil.copyJobConf( config );
    }

  @Override
  public <C> Map<String, String> diffConfigIntoMap( C defaultConfig, C updatedConfig )
    {
    return HadoopUtil.getConfig( (Configuration) defaultConfig, (Configuration) updatedConfig );
    }

  @Override
  public TezConfiguration mergeMapIntoConfig( TezConfiguration defaultConfig, Map<String, String> map )
    {
    return HadoopUtil.mergeConf( new TezConfiguration( defaultConfig ), map, true );
    }
  }
