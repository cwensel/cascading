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
import cascading.flow.FlowNode;
import cascading.flow.FlowStep;
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.stream.annotations.StreamMode;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.util.ProcessLogger;
import cascading.util.Util;

import static cascading.util.Util.createIdentitySet;

/**
 *
 */
public class BaseFlowNode implements Serializable, FlowNode, ProcessLogger
  {
  private final String id;
  private int ordinal;
  private String name;
  private Map<String, String> processAnnotations;

  private transient FlowStep flowStep;

  protected ElementGraph nodeSubGraph;
  protected List<? extends ElementGraph> pipelineGraphs = Collections.emptyList();

  private transient Set<FlowElement> sourceElements;
  private transient Set<FlowElement> sinkElements;
  private Map<String, Tap> trapMap;
  private Set<Tap> sourceTaps;
  private Set<Tap> sinkTaps;

  private Map<Tap, Set<String>> reverseSourceTaps;
  private Map<Tap, Set<String>> reverseSinkTaps;
  private Map<FlowElement, ElementGraph> streamPipelineMap = Collections.emptyMap();

  // for testing
  public BaseFlowNode( String name, int ordinal )
    {
    this.id = Util.createUniqueIDWhichStartsWithAChar(); // timeline server cannot filter strings that start with a number
    setName( name );
    this.ordinal = ordinal;
    }

  public BaseFlowNode( FlowElementGraph flowElementGraph, ElementGraph nodeSubGraph, List<? extends ElementGraph> pipelineGraphs )
    {
    this.id = Util.createUniqueIDWhichStartsWithAChar(); // timeline server cannot filter strings that start with a number
    this.nodeSubGraph = nodeSubGraph;

    if( pipelineGraphs != null )
      this.pipelineGraphs = pipelineGraphs;

    verifyPipelines();
    createPipelineMap();

    assignTrappableNames( flowElementGraph );
    assignTraps( flowElementGraph.getTrapMap() );
    }

  public void setOrdinal( int ordinal )
    {
    this.ordinal = ordinal;
    }

  @Override
  public int getOrdinal()
    {
    return ordinal;
    }

  @Override
  public String getID()
    {
    return id;
    }

  public void setName( String name )
    {
    this.name = name;
    }

  @Override
  public String getName()
    {
    return name;
    }

  @Override
  public Map<String, String> getProcessAnnotations()
    {
    if( processAnnotations == null )
      return Collections.emptyMap();

    return Collections.unmodifiableMap( processAnnotations );
    }

  @Override
  public void addProcessAnnotation( Enum annotation )
    {
    if( annotation == null )
      return;

    addProcessAnnotation( annotation.getDeclaringClass().getName(), annotation.name() );
    }

  @Override
  public void addProcessAnnotation( String key, String value )
    {
    if( processAnnotations == null )
      processAnnotations = new HashMap<>();

    processAnnotations.put( key, value );
    }

  public void setFlowStep( FlowStep flowStep )
    {
    this.flowStep = flowStep;
    }

  @Override
  public FlowStep getFlowStep()
    {
    return flowStep;
    }

  @Override
  public ElementGraph getElementGraph()
    {
    return nodeSubGraph;
    }

  @Override
  public Set<String> getSourceElementNames()
    {
    Set<String> results = new HashSet<>();

    for( FlowElement flowElement : getSourceElements() )
      {
      if( flowElement instanceof Tap )
        results.addAll( getSourceTapNames( (Tap) flowElement ) );
      else
        results.add( ( (Pipe) flowElement ).getName() );
      }

    return results;
    }

  public Set<FlowElement> getSourceElements()
    {
    if( sourceElements == null )
      sourceElements = Collections.unmodifiableSet( ElementGraphs.findSources( nodeSubGraph, FlowElement.class ) );

    return sourceElements;
    }

  @Override
  public Set<? extends FlowElement> getSourceElements( Enum annotation )
    {
    Set<? extends FlowElement> annotated = getFlowElementsFor( annotation );
    Set<FlowElement> sourceElements = getSourceElements();

    Set<FlowElement> results = new HashSet<>();

    for( FlowElement sourceElement : sourceElements )
      {
      if( annotated.contains( sourceElement ) )
        results.add( sourceElement );
      }

    return results;
    }

  @Override
  public Set<String> getSinkElementNames()
    {
    Set<String> results = new HashSet<>();

    for( FlowElement flowElement : getSinkElements() )
      {
      if( flowElement instanceof Tap )
        results.addAll( getSinkTapNames( (Tap) flowElement ) );
      else
        results.add( ( (Pipe) flowElement ).getName() );
      }

    return results;
    }

  @Override
  public Set<FlowElement> getSinkElements()
    {
    if( sinkElements == null )
      sinkElements = Collections.unmodifiableSet( ElementGraphs.findSinks( nodeSubGraph, FlowElement.class ) );

    return sinkElements;
    }

  public Set<? extends FlowElement> getSinkElements( Enum annotation )
    {
    Set<? extends FlowElement> annotated = getFlowElementsFor( annotation );
    Set<FlowElement> sinkElements = getSinkElements();

    Set<FlowElement> results = new HashSet<>();

    for( FlowElement sinkElement : sinkElements )
      {
      if( annotated.contains( sinkElement ) )
        results.add( sinkElement );
      }

    return results;
    }

  @Override
  public List<? extends ElementGraph> getPipelineGraphs()
    {
    return pipelineGraphs;
    }

  @Override
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
  public Set<Tap> getSourceTaps()
    {
    if( sourceTaps != null )
      return sourceTaps;

    sourceTaps = Collections.unmodifiableSet( Util.narrowSet( Tap.class, getSourceElements() ) );

    return sourceTaps;
    }

  @Override
  public Set<Tap> getSinkTaps()
    {
    if( sinkTaps != null )
      return sinkTaps;

    sinkTaps = Collections.unmodifiableSet( Util.narrowSet( Tap.class, getSinkElements() ) );

    return sinkTaps;
    }

  @Override
  public int getSubmitPriority()
    {
    return 0;
    }

  @Override
  public Set<String> getSourceTapNames( Tap source )
    {
    return reverseSourceTaps.get( source );
    }

  @Override
  public Set<String> getSinkTapNames( Tap sink )
    {
    return reverseSinkTaps.get( sink );
    }

  private void assignTrappableNames( FlowElementGraph flowElementGraph )
    {
    reverseSourceTaps = new HashMap<>();
    reverseSinkTaps = new HashMap<>();

    Set<Tap> sources = getSourceTaps();

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

    Set<Tap> sinks = getSinkTaps();

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
    if( !reverseSourceTaps.containsKey( source ) )
      reverseSourceTaps.put( source, new HashSet<String>() );

    reverseSourceTaps.get( source ).add( name );
    }

  private void addSinkName( String name, Tap sink )
    {
    if( !reverseSinkTaps.containsKey( sink ) )
      reverseSinkTaps.put( sink, new HashSet<String>() );

    reverseSinkTaps.get( sink ).add( name );
    }

  @Override
  public Map<String, Tap> getTrapMap()
    {
    return trapMap;
    }

  @Override
  public Collection<? extends Tap> getTraps()
    {
    return getTrapMap().values();
    }

  private void assignTraps( Map<String, Tap> traps )
    {
    trapMap = new HashMap<>();

    for( FlowElement flowElement : nodeSubGraph.vertexSet() )
      {
      Set<String> names = new HashSet<>();

      if( flowElement instanceof Extent )
        continue;

      if( flowElement instanceof Pipe )
        {
        names.add( ( (Pipe) flowElement ).getName() );
        }
      else
        {
        Set<String> sourceTapNames = getSourceTapNames( (Tap) flowElement );

        if( sourceTapNames != null )
          names.addAll( sourceTapNames );

        Set<String> sinkTapNames = getSinkTapNames( (Tap) flowElement );

        if( sinkTapNames != null )
          names.addAll( sinkTapNames );
        }

      for( String name : names )
        {
        if( traps.containsKey( name ) )
          trapMap.put( name, traps.get( name ) );
        }
      }
    }

  private void verifyPipelines()
    {
    if( pipelineGraphs == null || pipelineGraphs.isEmpty() )
      return;

    Set<FlowElement> allElements = createIdentitySet( nodeSubGraph.vertexSet() );

    for( ElementGraph pipelineGraph : pipelineGraphs )
      allElements.removeAll( pipelineGraph.vertexSet() );

    if( !allElements.isEmpty() )
      throw new IllegalStateException( "union of pipeline graphs for flow node are missing elements: " + Util.join( allElements, ", " ) );
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

  @Override
  public Tap getTrap( String branchName )
    {
    return trapMap.get( branchName );
    }

  @Override
  public Collection<? extends Scope> getPreviousScopes( FlowElement flowElement )
    {
    return nodeSubGraph.incomingEdgesOf( flowElement );
    }

  @Override
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

    BaseFlowNode flowNode = (BaseFlowNode) object;

    if( id != null ? !id.equals( flowNode.id ) : flowNode.id != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return id != null ? id.hashCode() : 0;
    }

  @Override
  public Set<? extends FlowElement> getFlowElementsFor( Enum annotation )
    {
    if( pipelineGraphs.isEmpty() )
      return ( (AnnotatedGraph) getElementGraph() ).getAnnotations().getValues( annotation );

    Set<FlowElement> results = createIdentitySet();

    for( ElementGraph pipelineGraph : pipelineGraphs )
      results.addAll( ( (AnnotatedGraph) pipelineGraph ).getAnnotations().getValues( annotation ) );

    return results;
    }

  private ProcessLogger getLogger()
    {
    if( flowStep != null && flowStep instanceof ProcessLogger )
      return (ProcessLogger) flowStep;

    return ProcessLogger.NULL;
    }

  @Override
  public boolean isInfoEnabled()
    {
    return getLogger().isInfoEnabled();
    }

  @Override
  public boolean isDebugEnabled()
    {
    return getLogger().isDebugEnabled();
    }

  @Override
  public void logInfo( String message, Object... arguments )
    {
    getLogger().logInfo( message, arguments );
    }

  @Override
  public void logDebug( String message, Object... arguments )
    {
    getLogger().logDebug( message, arguments );
    }

  @Override
  public void logWarn( String message )
    {
    getLogger().logWarn( message );
    }

  @Override
  public void logWarn( String message, Object... arguments )
    {
    getLogger().logWarn( message, arguments );
    }

  @Override
  public void logWarn( String message, Throwable throwable )
    {
    getLogger().logWarn( message, throwable );
    }

  @Override
  public void logError( String message, Object... arguments )
    {
    getLogger().logError( message, arguments );
    }

  @Override
  public void logError( String message, Throwable throwable )
    {
    getLogger().logError( message, throwable );
    }
  }
