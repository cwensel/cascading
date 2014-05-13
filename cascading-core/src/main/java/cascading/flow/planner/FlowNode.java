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

package cascading.flow.planner;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.stream.annotations.StreamMode;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.util.Util;

/**
 *
 */
public class FlowNode implements ProcessModel, Serializable
  {
  private final String id;
  private final int ordinal;
  private final String name;

  protected ElementGraph nodeSubGraph;
  protected List<ElementGraph> pipelineGraphs;

  private Set<FlowElement> sourceElements;
  private Set<FlowElement> sinkElements;
  private Map<String, Tap> trapMap;
  private Set<Tap> sourceTaps;
  private Set<Tap> sinkTaps;

  // sources accumulated by join
  private Map<Tap, Set<String>> reverseSource;
  private Map<Tap, Set<String>> reverseSink;
  private Map<FlowElement, ElementGraph> streamPipelineMap = Collections.emptyMap();

  public FlowNode( int ordinal, String name, FlowElementGraph flowElementGraph, ElementGraph nodeSubGraph, List<ElementGraph> pipelineGraphs )
    {
    this.id = Util.createUniqueID();
    this.ordinal = ordinal;
    this.name = name;
    this.nodeSubGraph = nodeSubGraph;
    this.pipelineGraphs = pipelineGraphs;

    verifyPipelines();
    createPipelineMap();

    assignTrappableNames( flowElementGraph );
    assignTraps( flowElementGraph.getTrapMap() );
    }

  @Override
  public int getOrdinal()
    {
    return ordinal;
    }

  public String getID()
    {
    return id;
    }

  @Override
  public String getName()
    {
    return name;
    }

  @Override
  public ElementGraph getElementGraph()
    {
    return nodeSubGraph;
    }

  public Set<FlowElement> getSourceElements()
    {
    if( sourceElements == null )
      sourceElements = ElementGraphs.findSources( nodeSubGraph, FlowElement.class );

    return sourceElements;
    }

  public Set<FlowElement> getSinkElements()
    {
    if( sinkElements == null )
      sinkElements = ElementGraphs.findSinks( nodeSubGraph, FlowElement.class );

    return sinkElements;
    }

  public ElementGraph getPipelineGraphFor( FlowElement streamedSource )
    {
    return streamPipelineMap.get( streamedSource );
    }

  @Override
  public Collection<Group> getGroups()
    {
    return ElementGraphs.findAllGroups( nodeSubGraph );
    }

  @Override
  public Set<Tap> getSources()
    {
    if( sourceTaps != null )
      return sourceTaps;

    sourceTaps = new HashSet<>();

    for( FlowElement sourceElement : getSourceElements() )
      {
      if( sourceElement instanceof Tap )
        sourceTaps.add( (Tap) sourceElement );
      }

    return sourceTaps;
    }

  @Override
  public Set<Tap> getSinks()
    {
    if( sinkTaps != null )
      return sinkTaps;

    sinkTaps = new HashSet<>();

    for( FlowElement sinkElement : getSinkElements() )
      {
      if( sinkElement instanceof Tap )
        sinkTaps.add( (Tap) sinkElement );
      }

    return sinkTaps;
    }

  @Override
  public int getSubmitPriority()
    {
    return 0;
    }

  public Set<String> getSourceNames( Tap source )
    {
    return reverseSource.get( source );
    }

  public Set<String> getSinkNames( Tap sink )
    {
    return reverseSink.get( sink );
    }

  private void assignTrappableNames( FlowElementGraph flowElementGraph )
    {
    reverseSource = new HashMap<>();
    reverseSink = new HashMap<>();

    Set<Tap> sources = getSources();

    for( Tap source : sources )
      {
      Set<Scope> scopes = flowElementGraph.outgoingEdgesOf( source );

      for( Scope scope : scopes )
        addSourceName( scope.getName(), source );
      }

    for( Map.Entry<String, Tap> entry : flowElementGraph.getSourceMap().entrySet() )
      {
      if( sources.contains( entry.getValue() ) )
        addSourceName( entry.getKey(), entry.getValue() );
      }

    Set<Tap> sinks = getSinks();

    for( Tap sink : sinks )
      {
      Set<Scope> scopes = flowElementGraph.incomingEdgesOf( sink );

      for( Scope scope : scopes )
        addSinkName( scope.getName(), sink );
      }

    for( Map.Entry<String, Tap> entry : flowElementGraph.getSinkMap().entrySet() )
      {
      if( sinks.contains( entry.getValue() ) )
        addSinkName( entry.getKey(), entry.getValue() );
      }
    }

  private void addSourceName( String name, Tap source )
    {
    if( !reverseSource.containsKey( source ) )
      reverseSource.put( source, new HashSet<String>() );

    reverseSource.get( source ).add( name );
    }

  private void addSinkName( String name, Tap sink )
    {
    if( !reverseSink.containsKey( sink ) )
      reverseSink.put( sink, new HashSet<String>() );

    reverseSink.get( sink ).add( name );
    }

  @Override
  public Map<String, Tap> getTrapMap()
    {
    return trapMap;
    }

  public Collection<Tap> getTraps()
    {
    return getTrapMap().values();
    }

  private void assignTraps( Map<String, Tap> traps )
    {
    trapMap = new HashMap<>();

    for( FlowElement flowElement : nodeSubGraph.vertexSet() )
      {
      if( !( flowElement instanceof Pipe ) )
        continue;

      String name = ( (Pipe) flowElement ).getName();

      // this is legacy, can probably now collapse into one collection safely
      if( traps.containsKey( name ) )
        trapMap.put( name, traps.get( name ) );
      }
    }

  private void verifyPipelines()
    {
    if( pipelineGraphs == null || pipelineGraphs.isEmpty() )
      return;

    Set<FlowElement> allElements = new HashSet<>( nodeSubGraph.vertexSet() );

    for( ElementGraph pipelineGraph : pipelineGraphs )
      allElements.removeAll( pipelineGraph.vertexSet() );

    if( !allElements.isEmpty() )
      throw new IllegalStateException( "union of pipeline graphs for flow node are missing elements: " + Util.join( allElements ) );
    }

  private void createPipelineMap()
    {
    if( pipelineGraphs == null || pipelineGraphs.isEmpty() )
      return;

    Map<FlowElement, ElementGraph> map = new HashMap<>( pipelineGraphs.size() );

    for( ElementGraph pipelineGraph : pipelineGraphs )
      {
      if( !( pipelineGraph instanceof AnnotatedGraph ) )
        throw new IllegalStateException( "pipeline graphs must be of type AnnotatedGraph, got: " + pipelineGraph.getClass().getName() );

      Set<FlowElement> flowElements;

      if( ( (AnnotatedGraph) pipelineGraph ).hasAnnotations() )
        flowElements = ( (AnnotatedGraph) pipelineGraph ).getAnnotations().getValues( StreamMode.Streamed );
      else
        flowElements = ElementGraphs.findSources( pipelineGraph, FlowElement.class );

      for( FlowElement flowElement : flowElements )
        {
        if( map.containsKey( flowElement ) )
          throw new IllegalStateException( "duplicate streamable elements, found:  " + flowElement );

        map.put( flowElement, pipelineGraph );
        }
      }

    this.streamPipelineMap = map;
    }

  public Tap getTrap( String branchName )
    {
    return trapMap.get( branchName );
    }

  public Collection<? extends Scope> getPreviousScopes( FlowElement flowElement )
    {
    return nodeSubGraph.incomingEdgesOf( flowElement );
    }

  public Collection<? extends Scope> getNextScopes( FlowElement flowElement )
    {
    return nodeSubGraph.outgoingEdgesOf( flowElement );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;

    if( object == null || getClass() != object.getClass() )
      return false;

    FlowNode flowNode = (FlowNode) object;

    if( id != null ? !id.equals( flowNode.id ) : flowNode.id != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return id != null ? id.hashCode() : 0;
    }

  public Collection<FlowElement> getFlowElementsFor( Enum annotation )
    {
    if( pipelineGraphs.isEmpty() )
      return ( (AnnotatedGraph) getElementGraph() ).getAnnotations().getValues( annotation );

    Set<FlowElement> results = new HashSet<>();

    for( ElementGraph pipelineGraph : pipelineGraphs )
      results.addAll( ( (AnnotatedGraph) pipelineGraph ).getAnnotations().getValues( annotation ) );

    return results;
    }

//
//  protected abstract Map<String, Config> initFromSources( FlowProcess<? extends Config> flowProcess, Config config );
//
//  protected abstract Map<String, Config> initFromSinks( FlowProcess<? extends Config> flowProcess, Config config );
//
//  protected abstract void initFromProcessConfigDef( Config config );
//
//  protected abstract void initFromTraps( FlowProcess<? extends Config> flowProcess, Config config );
//
//  protected FlowProcess getFlowProcess()
//    {
//    return flowStep.getFlow().getFlowProcess();
//    }
//
  }
