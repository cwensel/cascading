/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner.process;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.graph.Extent;
import cascading.pipe.Group;
import cascading.tap.Tap;
import cascading.util.EnumMultiMap;
import cascading.util.Util;
import cascading.util.jgrapht.IntegerNameProvider;
import cascading.util.jgrapht.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.Util.createIdentitySet;

/**
 *
 */
public abstract class BaseProcessGraph<Process extends ProcessModel> implements ProcessGraph<Process>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( BaseProcessGraph.class );

  final SimpleDirectedGraph<Process, ProcessEdge> graph;

  protected Set<FlowElement> sourceElements = createIdentitySet();
  protected Set<FlowElement> sinkElements = createIdentitySet();
  private Set<Tap> sourceTaps;
  private Set<Tap> sinkTaps;
  protected Map<String, Tap> trapsMap = new HashMap<>();

  public BaseProcessGraph()
    {
    graph = new SimpleDirectedGraph( ProcessEdge.class );
    }

  @Override
  public boolean addVertex( Process process )
    {
    sourceElements.addAll( process.getSourceElements() );
    sinkElements.addAll( process.getSinkElements() );
    trapsMap.putAll( process.getTrapMap() );

    return graph.addVertex( process );
    }

  protected void bindEdges()
    {
    for( Process sinkProcess : vertexSet() )
      {
      for( Process sourceProcess : vertexSet() )
        {
        if( sourceProcess == sinkProcess )
          continue;

        // outer edge sources and sinks to this graph
        sourceElements.removeAll( sinkProcess.getSinkElements() );
        sinkElements.removeAll( sourceProcess.getSourceElements() );
        }
      }

    for( Process sinkProcess : vertexSet() )
      {
      for( Process sourceProcess : vertexSet() )
        {
        if( sourceProcess == sinkProcess )
          continue;

        for( Object intermediate : sourceProcess.getSinkElements() )
          {
          if( sinkProcess.getSourceElements().contains( intermediate ) )
            addEdge( sourceProcess, sinkProcess, new ProcessEdge<>( sourceProcess, (FlowElement) intermediate, sinkProcess ) );
          }
        }
      }
    }

  @Override
  public Set<FlowElement> getSourceElements()
    {
    return sourceElements;
    }

  @Override
  public Set<FlowElement> getSinkElements()
    {
    return sinkElements;
    }

  @Override
  public Set<Tap> getSourceTaps()
    {
    if( sourceTaps != null )
      return sourceTaps;

    sourceTaps = Util.narrowIdentitySet( Tap.class, getSourceElements() );

    return sourceTaps;
    }

  @Override
  public Map<String, Tap> getSourceTapsMap()
    {
    Map<String, Tap> result = new HashMap<>();
    Set<Tap> sourceTaps = getSourceTaps();

    for( Tap sourceTap : sourceTaps )
      {
      for( Process process : graph.vertexSet() )
        {
        if( !process.getSourceTaps().contains( sourceTap ) )
          continue;

        ElementGraph elementGraph = process.getElementGraph();

        for( Scope scope : elementGraph.outgoingEdgesOf( sourceTap ) )
          result.put( scope.getName(), sourceTap );
        }
      }

    return result;
    }

  @Override
  public Set<Tap> getSinkTaps()
    {
    if( sinkTaps != null )
      return sinkTaps;

    sinkTaps = Util.narrowIdentitySet( Tap.class, getSinkElements() );

    return sinkTaps;
    }

  @Override
  public Map<String, Tap> getSinkTapsMap()
    {
    Map<String, Tap> result = new HashMap<>();
    Set<Tap> sinkTaps = getSinkTaps();

    for( Tap sinkTap : sinkTaps )
      {
      for( Process process : graph.vertexSet() )
        {
        if( !process.getSinkTaps().contains( sinkTap ) )
          continue;

        ElementGraph elementGraph = process.getElementGraph();

        for( Scope scope : elementGraph.incomingEdgesOf( sinkTap ) )
          result.put( scope.getName(), sinkTap );
        }
      }

    return result;
    }

  @Override
  public Map<String, Tap> getTrapsMap()
    {
    return trapsMap;
    }

  @Override
  public Iterator<Process> getTopologicalIterator()
    {
    return getOrderedTopologicalIterator( new Comparator<Process>()
    {
    @Override
    public int compare( Process lhs, Process rhs )
      {
      return Integer.valueOf( lhs.getSubmitPriority() ).compareTo( rhs.getSubmitPriority() );
      }
    } );
    }

  @Override
  public Iterator<Process> getOrdinalTopologicalIterator()
    {
    return getOrderedTopologicalIterator( new Comparator<Process>()
    {
    @Override
    public int compare( Process lhs, Process rhs )
      {
      return Integer.valueOf( lhs.getOrdinal() ).compareTo( rhs.getOrdinal() );
      }
    } );
    }

  @Override
  public Iterator<Process> getOrderedTopologicalIterator( Comparator<Process> comparator )
    {
    return new TopologicalOrderIterator<>( graph, new PriorityQueue<>( 10, comparator ) );
    }

  @Override
  public List<ElementGraph> getElementGraphs( FlowElement flowElement )
    {
    List<Process> elementProcesses = getElementProcesses( flowElement );

    List<ElementGraph> elementGraphs = new ArrayList<>();

    for( Process elementProcess : elementProcesses )
      elementGraphs.add( elementProcess.getElementGraph() );

    return elementGraphs;
    }

  @Override
  public List<Process> getElementProcesses( FlowElement flowElement )
    {
    List<Process> processes = new ArrayList<>();

    for( Process process : vertexSet() )
      {
      if( process.getElementGraph().vertexSet().contains( flowElement ) )
        processes.add( process );
      }

    return processes;
    }

  @Override
  public List<ElementGraph> getElementGraphs( Scope scope )
    {
    List<Process> elementProcesses = getElementProcesses( scope );

    List<ElementGraph> elementGraphs = new ArrayList<>();

    for( Process elementProcess : elementProcesses )
      elementGraphs.add( elementProcess.getElementGraph() );

    return elementGraphs;
    }

  @Override
  public List<Process> getElementProcesses( Scope scope )
    {
    List<Process> processes = new ArrayList<>();

    for( Process process : vertexSet() )
      {
      if( process.getElementGraph().edgeSet().contains( scope ) )
        processes.add( process );
      }

    return processes;
    }

  @Override
  public List<Process> getElementSourceProcesses( FlowElement flowElement )
    {
    List<Process> sources = new ArrayList<>();

    for( Process process : vertexSet() )
      {
      if( process.getSinkElements().contains( flowElement ) )
        sources.add( process );
      }

    return sources;
    }

  @Override
  public List<Process> getElementSinkProcesses( FlowElement flowElement )
    {
    List<Process> sinks = new ArrayList<>();

    for( Process process : vertexSet() )
      {
      if( process.getSourceElements().contains( flowElement ) )
        sinks.add( process );
      }

    return sinks;
    }

  @Override
  public Set<FlowElement> getAllSourceElements()
    {
    Set<FlowElement> results = createIdentitySet();

    for( Process process : vertexSet() )
      results.addAll( process.getSourceElements() );

    return results;
    }

  @Override
  public Set<FlowElement> getAllSinkElements()
    {
    Set<FlowElement> results = createIdentitySet();

    for( Process process : vertexSet() )
      results.addAll( process.getSinkElements() );

    return results;
    }

  public EnumMultiMap<FlowElement> getAnnotations()
    {
    EnumMultiMap<FlowElement> annotations = new EnumMultiMap<>();

    for( Process process : vertexSet() )
      {
      ElementGraph elementGraph = process.getElementGraph();

      if( elementGraph instanceof AnnotatedGraph )
        annotations.addAll( ( (AnnotatedGraph) elementGraph ).getAnnotations() );
      }

    return annotations;
    }

  /**
   * All elements, from the given ElementGraph, that belong to two or more processes, that are not sink or source elements that
   * connect processes.
   *
   * @return Set
   */
  @Override
  public Set<FlowElement> getDuplicatedElements( ElementGraph elementGraph )
    {
    Set<FlowElement> results = createIdentitySet();

    for( FlowElement flowElement : elementGraph.vertexSet() )
      {
      if( getElementProcesses( flowElement ).size() > 1 )
        results.add( flowElement );
      }

    results.remove( Extent.head );
    results.remove( Extent.tail );
    results.removeAll( getAllSourceElements() );
    results.removeAll( getAllSinkElements() );

    return results;
    }

  @Override
  public Set<ElementGraph> getIdentityElementGraphs()
    {
    Set<ElementGraph> results = createIdentitySet();

    for( Process process : getIdentityProcesses() )
      results.add( process.getElementGraph() );

    return results;
    }

  /**
   * Returns a set of processes that perform no internal operations.
   * <p/>
   * for example if a FlowNode only has a Merge source and a GroupBy sink.
   *
   * @return
   */
  @Override
  public Set<Process> getIdentityProcesses()
    {
    Set<Process> results = new HashSet<>();

    for( Process process : vertexSet() )
      {
      if( ProcessModels.isIdentity( process ) )
        results.add( process );
      }

    return results;
    }

  /**
   * Method writeDOT writes this element graph to a DOT file for easy visualization and debugging.
   *
   * @param filename of type String
   */
  @Override
  public void writeDOT( String filename )
    {
    printProcessGraph( filename );
    }

  protected void printProcessGraph( String filename )
    {
    try
      {
      Writer writer = new FileWriter( filename );

      Util.writeDOT( writer, graph, new IntegerNameProvider<Process>(), new VertexNameProvider<Process>()
      {
      public String getVertexName( Process process )
        {
        String name = "[" + process.getName() + "]";

        String sourceName = "";
        Set<Tap> sources = process.getSourceTaps();
        for( Tap source : sources )
          sourceName += "\\nsrc:[" + source.getIdentifier() + "]";

        if( sourceName.length() != 0 )
          name += sourceName;

        Collection<Group> groups = process.getGroups();

        for( Group group : groups )
          {
          String groupName = group.getName();

          if( groupName.length() != 0 )
            name += "\\ngrp:" + groupName;
          }

        Set<Tap> sinks = process.getSinkTaps();
        String sinkName = "";
        for( Tap sink : sinks )
          sinkName = "\\nsnk:[" + sink.getIdentifier() + "]";

        if( sinkName.length() != 0 )
          name += sinkName;

        return name.replaceAll( "\"", "\'" );
        }
      }, null );

      writer.close();
      }
    catch( IOException exception )
      {
      LOG.error( "failed printing graph to: {}, with exception: {}", filename, exception );
      }
    }

  @Override
  public void writeDOTNested( String filename, ElementGraph graph )
    {
    ElementGraphs.printProcessGraph( filename, graph, this );
    }

  public boolean containsEdge( Process sourceVertex, Process targetVertex )
    {
    return graph.containsEdge( sourceVertex, targetVertex );
    }

  public boolean removeAllEdges( Collection<? extends ProcessEdge> edges )
    {
    return graph.removeAllEdges( edges );
    }

  public Set<ProcessEdge> removeAllEdges( Process sourceVertex, Process targetVertex )
    {
    return graph.removeAllEdges( sourceVertex, targetVertex );
    }

  public boolean removeAllVertices( Collection<? extends Process> vertices )
    {
    return graph.removeAllVertices( vertices );
    }

  public Set<ProcessEdge> getAllEdges( Process sourceVertex, Process targetVertex )
    {
    return graph.getAllEdges( sourceVertex, targetVertex );
    }

  public ProcessEdge getEdge( Process sourceVertex, Process targetVertex )
    {
    return graph.getEdge( sourceVertex, targetVertex );
    }

  public ProcessEdge addEdge( Process sourceVertex, Process targetVertex )
    {
    return graph.addEdge( sourceVertex, targetVertex );
    }

  public boolean addEdge( Process sourceVertex, Process targetVertex, ProcessEdge processEdge )
    {
    return graph.addEdge( sourceVertex, targetVertex, processEdge );
    }

  public Process getEdgeSource( ProcessEdge processEdge )
    {
    return graph.getEdgeSource( processEdge );
    }

  public Process getEdgeTarget( ProcessEdge processEdge )
    {
    return graph.getEdgeTarget( processEdge );
    }

  public boolean containsEdge( ProcessEdge processEdge )
    {
    return graph.containsEdge( processEdge );
    }

  public boolean containsVertex( Process process )
    {
    return graph.containsVertex( process );
    }

  public Set<ProcessEdge> edgeSet()
    {
    return graph.edgeSet();
    }

  public Set<ProcessEdge> edgesOf( Process vertex )
    {
    return graph.edgesOf( vertex );
    }

  public int inDegreeOf( Process vertex )
    {
    return graph.inDegreeOf( vertex );
    }

  public Set<ProcessEdge> incomingEdgesOf( Process vertex )
    {
    return graph.incomingEdgesOf( vertex );
    }

  public int outDegreeOf( Process vertex )
    {
    return graph.outDegreeOf( vertex );
    }

  public Set<ProcessEdge> outgoingEdgesOf( Process vertex )
    {
    return graph.outgoingEdgesOf( vertex );
    }

  public ProcessEdge removeEdge( Process sourceVertex, Process targetVertex )
    {
    return graph.removeEdge( sourceVertex, targetVertex );
    }

  public boolean removeEdge( ProcessEdge processEdge )
    {
    return graph.removeEdge( processEdge );
    }

  public boolean removeVertex( Process process )
    {
    return graph.removeVertex( process );
    }

  public Set<Process> vertexSet()
    {
    return graph.vertexSet();
    }

  public double getEdgeWeight( ProcessEdge processEdge )
    {
    return graph.getEdgeWeight( processEdge );
    }
  }
