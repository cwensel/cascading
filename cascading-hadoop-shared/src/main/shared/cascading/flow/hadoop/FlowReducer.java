/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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
import java.util.Iterator;

import cascading.CascadingException;
import cascading.flow.FlowException;
import cascading.flow.FlowSession;
import cascading.flow.FlowStep;
import cascading.flow.SliceCounters;
import cascading.flow.hadoop.stream.HadoopGroupGate;
import cascading.flow.hadoop.stream.HadoopReduceStreamGraph;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.hadoop.util.TimedIterator;
import cascading.flow.stream.Duct;
import cascading.flow.stream.ElementDuct;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.hadoop.util.HadoopUtil.deserializeBase64;
import static cascading.flow.hadoop.util.HadoopUtil.readStateFromDistCache;

/** Class FlowReducer is the Hadoop Reducer implementation. */
public class FlowReducer extends MapReduceBase implements Reducer
  {
  private static final Logger LOG = LoggerFactory.getLogger( FlowReducer.class );

  /** Field flowReducerStack */
  private HadoopReduceStreamGraph streamGraph;
  /** Field currentProcess */
  private HadoopFlowProcess currentProcess;
  private TimedIterator timedIterator;

  private boolean calledPrepare = false;
  private HadoopGroupGate group;

  /** Constructor FlowReducer creates a new FlowReducer instance. */
  public FlowReducer()
    {
    }

  @Override
  public void configure( JobConf jobConf )
    {
    try
      {
      super.configure( jobConf );
      HadoopUtil.initLog4j( jobConf );

      LOG.info( "cascading version: {}", jobConf.get( "cascading.version", "" ) );
      LOG.info( "child jvm opts: {}", jobConf.get( "mapred.child.java.opts", "" ) );

      currentProcess = new HadoopFlowProcess( new FlowSession(), jobConf, false );

      timedIterator = new TimedIterator( currentProcess, SliceCounters.Read_Duration, SliceCounters.Tuples_Read );

      String stepState = jobConf.getRaw( "cascading.flow.step" );

      if( stepState == null )
        stepState = readStateFromDistCache( jobConf, jobConf.get( FlowStep.CASCADING_FLOW_STEP_ID ) );

      HadoopFlowStep step = deserializeBase64( stepState, jobConf, HadoopFlowStep.class );

      streamGraph = new HadoopReduceStreamGraph( currentProcess, step );

      group = (HadoopGroupGate) streamGraph.getHeads().iterator().next();

      for( Duct head : streamGraph.getHeads() )
        LOG.info( "sourcing from: " + ( (ElementDuct) head ).getFlowElement() );

      for( Duct tail : streamGraph.getTails() )
        LOG.info( "sinking to: " + ( (ElementDuct) tail ).getFlowElement() );

      for( Tap trap : step.getReducerTraps().values() )
        LOG.info( "trapping to: " + trap );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during reducer configuration", throwable );
      }
    }

  public void reduce( Object key, Iterator values, OutputCollector output, Reporter reporter ) throws IOException
    {
    currentProcess.setReporter( reporter );
    currentProcess.setOutputCollector( output );

    timedIterator.reset( values ); // allows us to count read tuples

    if( !calledPrepare )
      {
      currentProcess.increment( SliceCounters.Process_Begin_Time, System.currentTimeMillis() );

      streamGraph.prepare();

      calledPrepare = true;

      group.start( group );
      }

    try
      {
      group.run( (Tuple) key, timedIterator );
      }
    catch( OutOfMemoryError error )
      {
      throw error;
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during reducer execution", throwable );
      }
    }

  @Override
  public void close() throws IOException
    {
    try
      {
      if( calledPrepare )
        {
        group.complete( group );

        streamGraph.cleanup();
        }

      super.close();
      }
    finally
      {
      if( currentProcess != null )
        currentProcess.increment( SliceCounters.Process_End_Time, System.currentTimeMillis() );
      }
    }
  }
