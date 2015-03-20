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

package cascading.flow.hadoop;

import java.io.IOException;
import java.util.Iterator;

import cascading.CascadingException;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.FlowSession;
import cascading.flow.FlowStep;
import cascading.flow.Flows;
import cascading.flow.SliceCounters;
import cascading.flow.hadoop.planner.HadoopFlowStepJob;
import cascading.flow.hadoop.stream.graph.HadoopMapStreamGraph;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.BaseFlowNode;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.ElementDuct;
import cascading.flow.stream.element.SourceStage;
import cascading.tap.Tap;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.hadoop.util.HadoopMRUtil.readStateFromDistCache;
import static cascading.flow.hadoop.util.HadoopUtil.deserializeBase64;

/** Class FlowMapper is the Hadoop Mapper implementation. */
public class FlowMapper implements MapRunnable
  {
  private static final Logger LOG = LoggerFactory.getLogger( FlowMapper.class );

  private HadoopMapStreamGraph streamGraph;
  /** Field currentProcess */
  private HadoopFlowProcess currentProcess;

  /** Constructor FlowMapper creates a new FlowMapper instance. */
  public FlowMapper()
    {
    }

  @Override
  public void configure( JobConf jobConf )
    {
    try
      {
      HadoopUtil.initLog4j( jobConf );

      LOG.info( "cascading version: {}", jobConf.get( "cascading.version", "" ) );
      LOG.info( "child jvm opts: {}", jobConf.get( "mapred.child.java.opts", "" ) );

      currentProcess = new HadoopFlowProcess( new FlowSession(), jobConf, true );

      String mapNodeState = jobConf.getRaw( "cascading.flow.step.node.map" );

      if( mapNodeState == null )
        mapNodeState = readStateFromDistCache( jobConf, jobConf.get( FlowStep.CASCADING_FLOW_STEP_ID ), "map" );

      FlowNode node = deserializeBase64( mapNodeState, jobConf, BaseFlowNode.class );

      Tap source = Flows.getTapForID( node.getSourceTaps(), jobConf.get( "cascading.step.source" ) );

      streamGraph = new HadoopMapStreamGraph( currentProcess, node, source );

      for( Duct head : streamGraph.getHeads() )
        LOG.info( "sourcing from: " + ( (ElementDuct) head ).getFlowElement() );

      for( Duct tail : streamGraph.getTails() )
        LOG.info( "sinking to: " + ( (ElementDuct) tail ).getFlowElement() );

      for( Tap trap : node.getTraps() )
        LOG.info( "trapping to: " + trap );
      }
    catch( Throwable throwable )
      {
      reportIfLocal( throwable );

      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during mapper configuration", throwable );
      }
    }

  @Override
  public void run( RecordReader input, OutputCollector output, Reporter reporter ) throws IOException
    {
    currentProcess.setReporter( reporter );
    currentProcess.setOutputCollector( output );

    streamGraph.prepare();

    long processBeginTime = System.currentTimeMillis();

    currentProcess.increment( SliceCounters.Process_Begin_Time, processBeginTime );

    SourceStage streamedHead = streamGraph.getStreamedHead();
    Iterator<Duct> iterator = streamGraph.getHeads().iterator();

    try
      {
      try
        {
        while( iterator.hasNext() )
          {
          Duct next = iterator.next();

          if( next != streamedHead )
            ( (SourceStage) next ).run( null );
          }

        streamedHead.run( input );
        }
      catch( OutOfMemoryError error )
        {
        throw error;
        }
      catch( IOException exception )
        {
        reportIfLocal( exception );
        throw exception;
        }
      catch( Throwable throwable )
        {
        reportIfLocal( throwable );

        if( throwable instanceof CascadingException )
          throw (CascadingException) throwable;

        throw new FlowException( "internal error during mapper execution", throwable );
        }
      }
    finally
      {
      try
        {
        streamGraph.cleanup();
        }
      finally
        {
        long processEndTime = System.currentTimeMillis();

        currentProcess.increment( SliceCounters.Process_End_Time, processEndTime );
        currentProcess.increment( SliceCounters.Process_Duration, processEndTime - processBeginTime );
        }
      }
    }

  /**
   * Report the error to HadoopFlowStepJob if we are running in Hadoops local mode.
   *
   * @param throwable The throwable that was thrown.
   */
  private void reportIfLocal( Throwable throwable )
    {
    if( HadoopUtil.isLocal( currentProcess.getJobConf() ) )
      HadoopFlowStepJob.reportLocalError( throwable );
    }
  }
