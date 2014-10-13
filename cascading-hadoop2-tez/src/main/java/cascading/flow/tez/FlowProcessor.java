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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cascading.CascadingException;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.FlowSession;
import cascading.flow.SliceCounters;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.BaseFlowNode;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.ElementDuct;
import cascading.flow.stream.element.InputSource;
import cascading.flow.tez.stream.graph.Hadoop2TezStreamGraph;
import cascading.tap.Tap;
import cascading.util.Util;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.hadoop.util.HadoopUtil.deserializeBase64;

/**
 *
 */
public class FlowProcessor extends AbstractLogicalIOProcessor
  {
  private static final Logger LOG = LoggerFactory.getLogger( FlowProcessor.class );

  private TezConfiguration configuration;
  private Hadoop2TezFlowProcess currentProcess;
  private FlowNode flowNode;
  private Hadoop2TezStreamGraph streamGraph;

  public FlowProcessor( ProcessorContext context )
    {
    super( context );
    }

  @Override
  public void initialize() throws Exception
    {
    configuration = new TezConfiguration( TezUtils.createConfFromUserPayload( getContext().getUserPayload() ) );

    try
      {
      HadoopUtil.initLog4j( configuration );

      LOG.info( "cascading version: {}", configuration.get( "cascading.version", "" ) );
      // print jvm settings
//      LOG.info( "child jvm opts: {}", configuration.get( "mapred.child.java.opts", "" ) );

      currentProcess = new Hadoop2TezFlowProcess( new FlowSession(), getContext(), configuration );

      flowNode = deserializeBase64( configuration.getRaw( FlowNode.CASCADING_FLOW_NODE ), configuration, BaseFlowNode.class );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during processor configuration", throwable );
      }
    }

  @Override
  public void run( Map<String, LogicalInput> inputMap, Map<String, LogicalOutput> outputMap ) throws Exception
    {
    Collection<Duct> allHeads;
    InputSource streamedHead;

    try
      {
      streamGraph = new Hadoop2TezStreamGraph( currentProcess, flowNode, inputMap, outputMap );

      allHeads = streamGraph.getHeads();
      streamedHead = streamGraph.getStreamedHead();

      for( Duct head : allHeads )
        LOG.info( "sourcing from: {} streamed: {}", ( (ElementDuct) head ).getFlowElement(), head == streamedHead );

      for( Duct tail : streamGraph.getTails() )
        LOG.info( "sinking to: {}", ( (ElementDuct) tail ).getFlowElement() );

      for( Tap trap : flowNode.getTraps() )
        LOG.info( "trapping to: {}", trap );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during processor configuration", throwable );
      }

    streamGraph.prepare(); // starts inputs

    // wait for shuffle
    waitForInputsRead( inputMap );

    // user code begins executing from here
    long processBeginTime = System.currentTimeMillis();

    currentProcess.increment( SliceCounters.Process_Begin_Time, processBeginTime );

    Iterator<Duct> iterator = allHeads.iterator();

    try
      {
      try
        {
        while( iterator.hasNext() )
          {
          Duct next = iterator.next();

          if( next != streamedHead )
            ( (InputSource) next ).run( null );
          }

        streamedHead.run( null );
        }
      catch( OutOfMemoryError | IOException error )
        {
        throw error;
        }
      catch( Throwable throwable )
        {
        if( throwable instanceof CascadingException )
          throw (CascadingException) throwable;

        throw new FlowException( "internal error during processor execution on node: " + flowNode.getOrdinal(), throwable );
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

  protected void waitForInputsRead( Map<String, LogicalInput> inputMap ) throws InterruptedException
    {
    long beginInputReady = System.currentTimeMillis();

    getContext().waitForAllInputsReady( new HashSet<Input>( inputMap.values() ) );

    LOG.info( "all inputs ready in: {}", Util.formatDurationHMSms( System.currentTimeMillis() - beginInputReady ) );
    }

  @Override
  public void handleEvents( List<Event> events )
    {
    LOG.info( "in events" );
    }

  @Override
  public void close() throws Exception
    {
    LOG.info( "in close" );
    }
  }
