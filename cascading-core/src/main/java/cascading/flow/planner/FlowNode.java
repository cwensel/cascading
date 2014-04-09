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

package cascading.flow.planner;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.graph.ElementGraph;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.util.Util;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.KShortestPaths;

import static cascading.flow.planner.ElementGraphs.*;

/**
 *
 */
public class FlowNode implements ProcessModel, Serializable
  {
  private final String id;
  private final int ordinal;
  private final String name;

  protected ElementGraph nodeSubGraph;

  private Set<FlowElement> sourceElements;
  private Set<FlowElement> sinkElements;
  private Map<String, Tap> trapMap;
  private Set<Tap> sourceTaps;
  private Set<Tap> sinkTaps;

  // sources streamed into join - not necessarily all sources
  protected final Map<HashJoin, Tap> streamedSourceByJoin = new LinkedHashMap<>();
  // sources accumulated by join
  protected final Map<HashJoin, Set<Tap>> accumulatedSourcesByJoin = new LinkedHashMap<>();
  private Map<Tap, Set<String>> reverseSource;
  private Map<Tap, Set<String>> reverseSink;

  public FlowNode( int ordinal, String name, FlowElementGraph flowElementGraph, ElementGraph nodeSubGraph )
    {
    this.id = Util.createUniqueID();
    this.ordinal = ordinal;
    this.name = name;
    this.nodeSubGraph = nodeSubGraph;

    assignTrappableNames( flowElementGraph );
    assignTraps( flowElementGraph.getTrapMap() );
    addSourceModes();
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

  public Map<HashJoin, Tap> getStreamedSourceByJoin()
    {
    return streamedSourceByJoin;
    }

  public void addStreamedSourceFor( HashJoin join, Tap streamedSource )
    {
    streamedSourceByJoin.put( join, streamedSource );
    }

  public Set<Tap> getAllAccumulatedSources()
    {
    HashSet<Tap> set = new HashSet<Tap>();

    for( Set<Tap> taps : accumulatedSourcesByJoin.values() )
      set.addAll( taps );

    return set;
    }

  public void addAccumulatedSourceFor( HashJoin join, Tap accumulatedSource )
    {
    if( !accumulatedSourcesByJoin.containsKey( join ) )
      accumulatedSourcesByJoin.put( join, new HashSet<Tap>() );

    accumulatedSourcesByJoin.get( join ).add( accumulatedSource );
    }

  public Set<Tap> getJoinTributariesBetween( FlowElement from, FlowElement to )
    {
    Set<HashJoin> joins = new HashSet<HashJoin>();
    Set<Merge> merges = new HashSet<Merge>();

    List<GraphPath<FlowElement, Scope>> paths = getPathsBetween( from, to );

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      for( FlowElement flowElement : Graphs.getPathVertexList( path ) )
        {
        if( flowElement instanceof HashJoin )
          joins.add( (HashJoin) flowElement );

        if( flowElement instanceof Merge )
          merges.add( (Merge) flowElement );
        }
      }

    Set<Tap> tributaries = new HashSet<>();

    for( HashJoin join : joins )
      {
      for( Tap source : getSources() )
        {
        List<GraphPath<FlowElement, Scope>> joinPaths = new LinkedList<>( getPathsBetween( source, join ) );

        ListIterator<GraphPath<FlowElement, Scope>> iterator = joinPaths.listIterator();

        while( iterator.hasNext() )
          {
          GraphPath<FlowElement, Scope> joinPath = iterator.next();

          if( !Collections.disjoint( Graphs.getPathVertexList( joinPath ), merges ) )
            iterator.remove();
          }

        if( !joinPaths.isEmpty() )
          tributaries.add( source );
        }
      }

    return tributaries;
    }

  private List<GraphPath<FlowElement, Scope>> getPathsBetween( FlowElement from, FlowElement to )
    {
    KShortestPaths<FlowElement, Scope> paths = new KShortestPaths<FlowElement, Scope>( getElementGraph(), from, Integer.MAX_VALUE );
    List<GraphPath<FlowElement, Scope>> results = paths.getPaths( to );

    if( results == null )
      return Collections.EMPTY_LIST;

    return results;
    }

  private void addSourceModes()
    {
    Set<HashJoin> hashJoins = ElementGraphs.findAllHashJoins( getElementGraph() );

    for( HashJoin hashJoin : hashJoins )
      {
      for( Object object : getSources() )
        {
        Tap source = (Tap) object;
        Map<Integer, Integer> sourcePaths = countOrderedDirectPathsBetween( getElementGraph(), source, hashJoin );

        boolean isStreamed = isOnlyStreamedPath( sourcePaths );
        boolean isAccumulated = isOnlyAccumulatedPath( sourcePaths );
        boolean isBoth = isBothAccumulatedAndStreamedPath( sourcePaths );

        if( isStreamed || isBoth )
          addStreamedSourceFor( hashJoin, source );

        if( isAccumulated || isBoth )
          addAccumulatedSourceFor( hashJoin, source );
        }
      }
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

  public List<FlowElement> getSuccessors( FlowElement flowElement )
    {
    return Graphs.successorListOf( nodeSubGraph, flowElement );
    }

  public List<FlowElement> getPredecessor( FlowElement flowElement )
    {
    return Graphs.predecessorListOf( nodeSubGraph, flowElement );
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
