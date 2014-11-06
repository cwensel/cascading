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

package cascading.flow.tez.stream.graph;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowElements;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.Flows;
import cascading.flow.hadoop.stream.HadoopMemoryJoinGate;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.stream.annotations.StreamMode;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Gate;
import cascading.flow.stream.element.InputSource;
import cascading.flow.stream.element.MemoryHashJoinGate;
import cascading.flow.stream.element.SinkStage;
import cascading.flow.stream.element.SourceStage;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.NodeStreamGraph;
import cascading.flow.tez.Hadoop2TezFlowProcess;
import cascading.flow.tez.stream.element.TezBoundaryStage;
import cascading.flow.tez.stream.element.TezCoGroupGate;
import cascading.flow.tez.stream.element.TezGroupByGate;
import cascading.flow.tez.stream.element.TezMergeGate;
import cascading.flow.tez.stream.element.TezSinkStage;
import cascading.flow.tez.stream.element.TezSourceStage;
import cascading.flow.tez.util.TezUtil;
import cascading.pipe.Boundary;
import cascading.pipe.CoGroup;
import cascading.pipe.Group;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.util.SetMultiMap;
import cascading.util.SortedListMultiMap;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.tez.util.TezUtil.*;

/**
 *
 */
public class Hadoop2TezStreamGraph extends NodeStreamGraph
  {
  private static final Logger LOG = LoggerFactory.getLogger( Hadoop2TezStreamGraph.class );

  private InputSource streamedHead;
  private Map<String, LogicalInput> inputMap;
  private Map<String, LogicalOutput> outputMap;
  private Map<LogicalInput, Configuration> inputConfigMap = new HashMap<>();
  private Map<LogicalOutput, Configuration> outputConfigMap = new HashMap<>();
  private SetMultiMap<String, LogicalInput> inputMultiMap;
  private SetMultiMap<String, LogicalOutput> outputMultiMap;

  public Hadoop2TezStreamGraph( Hadoop2TezFlowProcess currentProcess, FlowNode flowNode, Map<String, LogicalInput> inputMap, Map<String, LogicalOutput> outputMap )
    {
    super( currentProcess, flowNode );
    this.inputMap = inputMap;
    this.outputMap = outputMap;

    buildGraph();

    setTraps();
    setScopes();

    printGraph( node.getID(), node.getName(), flowProcess.getCurrentSliceNum() );
    bind();
    }

  public InputSource getStreamedHead()
    {
    return streamedHead;
    }

  protected void buildGraph()
    {
    inputMultiMap = new SetMultiMap<>();

    for( Map.Entry<String, LogicalInput> entry : inputMap.entrySet() )
      {
      Configuration inputConfiguration = getInputConfiguration( entry.getValue() );
      inputConfigMap.put( entry.getValue(), inputConfiguration );

      inputMultiMap.addAll( getEdgeSourceID( entry.getValue(), inputConfiguration ), entry.getValue() );
      }

    outputMultiMap = new SetMultiMap<>();

    for( Map.Entry<String, LogicalOutput> entry : outputMap.entrySet() )
      {
      Configuration outputConfiguration = getOutputConfiguration( entry.getValue() );
      outputConfigMap.put( entry.getValue(), outputConfiguration );

      outputMultiMap.addAll( TezUtil.getEdgeSinkID( entry.getValue(), outputConfiguration ), entry.getValue() );
      }

    // this made the assumption we can have a physical and logical input per vertex. seems we can't
    if( inputMultiMap.getKeys().size() == 1 )
      {
      streamedSource = Flows.getFlowElementForID( node.getSourceElements(), Util.getFirst( inputMultiMap.getKeys() ) );
      }
    else
      {
      Set<FlowElement> sourceElements = new HashSet<>( node.getSourceElements() );
      Set<? extends FlowElement> accumulated = node.getSourceElements( StreamMode.Accumulated );

      sourceElements.removeAll( accumulated );

      if( sourceElements.size() != 1 )
        throw new IllegalStateException( "too many input source keys, got: " + Util.join( sourceElements ) );

      streamedSource = Util.getFirst( sourceElements );
      }

    LOG.info( "using streamed source: " + streamedSource );

    streamedHead = handleHead( streamedSource, flowProcess );

    Set<FlowElement> accumulated = new HashSet<>( node.getSourceElements() );

    accumulated.remove( streamedSource );

    Hadoop2TezFlowProcess tezProcess = (Hadoop2TezFlowProcess) flowProcess;
    TezConfiguration conf = tezProcess.getConfiguration();

    for( FlowElement flowElement : accumulated )
      {
      LOG.info( "using accumulated source: " + flowElement );

      if( flowElement instanceof Tap )
        {
        Tap source = (Tap) flowElement;

        // allows client side config to be used cluster side
        String property = conf.getRaw( "cascading.node.accumulated.source.conf." + Tap.id( source ) );

        if( property == null )
          throw new IllegalStateException( "accumulated source conf property missing for: " + source.getIdentifier() );

        conf = getSourceConf( tezProcess, conf, property );
        }
      else
        {
        conf = (TezConfiguration) inputConfigMap.get( FlowElements.id( flowElement ) );
        }

      FlowProcess flowProcess = conf == null ? tezProcess : new Hadoop2TezFlowProcess( tezProcess, conf );

      handleHead( flowElement, flowProcess );
      }
    }

  private TezConfiguration getSourceConf( FlowProcess<TezConfiguration> flowProcess, TezConfiguration conf, String property )
    {
    Map<String, String> priorConf;

    try
      {
      priorConf = (Map<String, String>) HadoopUtil.deserializeBase64( property, conf, HashMap.class, true );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to deserialize properties", exception );
      }

    return flowProcess.mergeMapIntoConfig( conf, priorConf );
    }

  private InputSource handleHead( FlowElement source, FlowProcess flowProcess )
    {
    Duct sourceDuct;

    if( source instanceof Tap )
      sourceDuct = createSourceStage( (Tap) source, flowProcess );
    else if( source instanceof Merge )
      sourceDuct = createMergeStage( (Merge) source, IORole.source );
    else if( source instanceof Boundary )
      sourceDuct = createBoundaryStage( (Boundary) source, IORole.source );
    else if( ( (Group) source ).isGroupBy() )
      sourceDuct = createGroupByGate( (GroupBy) source, IORole.source );
    else
      sourceDuct = createCoGroupGate( (CoGroup) source, IORole.source );

    addHead( sourceDuct );

    handleDuct( source, sourceDuct );

    return (InputSource) sourceDuct;
    }

  protected SourceStage createSourceStage( Tap source, FlowProcess flowProcess )
    {
    String id = Tap.id( source );
    LogicalInput logicalInput = inputMap.get( id );

    if( logicalInput == null )
      logicalInput = inputMap.get( flowProcess.getStringProperty( "cascading.node.source." + id ) );

    if( logicalInput == null )
      return new SourceStage( flowProcess, source );

    return new TezSourceStage( flowProcess, source, logicalInput );
    }

  @Override
  protected SinkStage createSinkStage( Tap sink )
    {
    String id = Tap.id( sink );
    LogicalOutput logicalOutput = outputMap.get( id );

    if( logicalOutput == null )
      logicalOutput = outputMap.get( flowProcess.getStringProperty( "cascading.node.sink." + id ) );

    if( logicalOutput == null )
      throw new IllegalStateException( "could not find output for: " + sink );

    return new TezSinkStage( flowProcess, sink, logicalOutput );
    }

  @Override
  protected Duct createMergeStage( Merge element, IORole role )
    {
    if( role == IORole.pass )
      return super.createMergeStage( element, IORole.pass );
    else if( role == IORole.sink )
      return createSinkMergeGate( element );
    else if( role == IORole.source )
      return createSourceMergeGate( element );
    else
      throw new UnsupportedOperationException( "both role not supported with merge" );
    }

  private Duct createSourceMergeGate( Merge element )
    {
    return new TezMergeGate( flowProcess, element, IORole.source, createInputMap( element ) );
    }

  private Duct createSinkMergeGate( Merge element )
    {
    return new TezMergeGate( flowProcess, element, IORole.sink, findLogicalOutputs( element ) );
    }

  @Override
  protected Duct createBoundaryStage( Boundary element, IORole role )
    {
    if( role == IORole.pass )
      return super.createBoundaryStage( element, IORole.pass );
    else if( role == IORole.sink )
      return createSinkBoundaryStage( element );
    else if( role == IORole.source )
      return createSourceBoundaryStage( element );
    else
      throw new UnsupportedOperationException( "both role not supported with boundary" );
    }

  private Duct createSourceBoundaryStage( Boundary element )
    {
    return new TezBoundaryStage( flowProcess, element, IORole.source, findLogicalInput( element ) );
    }

  private Duct createSinkBoundaryStage( Boundary element )
    {
    return new TezBoundaryStage( flowProcess, element, IORole.sink, findLogicalOutputs( element ) );
    }

  @Override
  protected Gate createGroupByGate( GroupBy element, IORole role )
    {
    if( role == IORole.sink )
      return createSinkGroupByGate( element );
    else
      return createSourceGroupByGate( element );
    }

  @Override
  protected Gate createCoGroupGate( CoGroup element, IORole role )
    {
    if( role == IORole.sink )
      return createSinkCoGroupByGate( element );
    else
      return createSourceCoGroupByGate( element );
    }

  private Gate createSinkCoGroupByGate( CoGroup element )
    {
    return new TezCoGroupGate( flowProcess, element, IORole.sink, findLogicalOutput( element ) );
    }

  private Gate createSourceCoGroupByGate( CoGroup element )
    {
    return new TezCoGroupGate( flowProcess, element, IORole.source, createInputMap( element ) );
    }

  protected Gate createSinkGroupByGate( GroupBy element )
    {
    return new TezGroupByGate( flowProcess, element, IORole.sink, findLogicalOutput( element ) );
    }

  protected Gate createSourceGroupByGate( GroupBy element )
    {
    return new TezGroupByGate( flowProcess, element, IORole.source, createInputMap( element ) );
    }

  private LogicalOutput findLogicalOutput( Pipe element )
    {
    String id = Pipe.id( element );
    LogicalOutput logicalOutput = outputMap.get( id );

    if( logicalOutput == null )
      logicalOutput = outputMap.get( flowProcess.getStringProperty( "cascading.node.sink." + id ) );

    if( logicalOutput == null )
      throw new IllegalStateException( "could not find output for: " + element );

    return logicalOutput;
    }

  private Collection<LogicalOutput> findLogicalOutputs( Pipe element )
    {
    String id = Pipe.id( element );

    return outputMultiMap.getValues( id );
    }

  private LogicalInput findLogicalInput( Pipe element )
    {
    String id = Pipe.id( element );
    LogicalInput logicalInput = inputMap.get( id );

    if( logicalInput == null )
      logicalInput = inputMap.get( flowProcess.getStringProperty( "cascading.node.source." + id ) );

    if( logicalInput == null )
      throw new IllegalStateException( "could not find input for: " + element );

    return logicalInput;
    }

  /**
   * Maps each input to an ordinal on the flowelement. an input may be bound to multiple ordinals.
   *
   * @param element
   */
  private SortedListMultiMap<Integer, LogicalInput> createInputMap( FlowElement element )
    {
    String id = FlowElements.id( element );
    SortedListMultiMap<Integer, LogicalInput> ordinalMap = new SortedListMultiMap<>();

    for( LogicalInput logicalInput : inputMap.values() )
      {
      Configuration configuration = inputConfigMap.get( logicalInput );

      String foundID = configuration.get( "cascading.node.source" );

      if( Util.isEmpty( foundID ) )
        throw new IllegalStateException( "cascading.node.source property not set on source LogicalInput" );

      if( !foundID.equals( id ) )
        continue;

      String values = configuration.get( "cascading.node.source.ordinals", "" );
      List<Integer> ordinals = Util.split( Integer.class, ",", values );

      for( Integer ordinal : ordinals )
        ordinalMap.put( ordinal, logicalInput );
      }

    return ordinalMap;
    }

  @Override
  protected MemoryHashJoinGate createNonBlockingJoinGate( HashJoin join )
    {
    return new HadoopMemoryJoinGate( flowProcess, join ); // does not use a latch
    }
  }
