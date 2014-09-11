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

package cascading.flow.planner.process;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowElements;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.pipe.Group;
import cascading.tap.Tap;
import cascading.util.Util;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class ProcessGraph<Process extends ProcessModel> extends SimpleDirectedGraph<Process, ProcessGraph.ProcessEdge>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( ProcessGraph.class );

  protected Set<FlowElement> sourceElements = new HashSet<>();
  protected Set<FlowElement> sinkElements = new HashSet<>();
  private Set<Tap> sourceTaps;
  private Set<Tap> sinkTaps;
  protected Map<String, Tap> trapsMap = new HashMap<>();

  public ProcessGraph()
    {
    super( ProcessEdge.class );
    }

  @Override
  public boolean addVertex( Process process )
    {
    sourceElements.addAll( process.getSourceElements() );
    sinkElements.addAll( process.getSinkElements() );
    trapsMap.putAll( process.getTrapMap() );

    return super.addVertex( process );
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

        for( Object sink : sinkProcess.getSinkElements() )
          {
          if( sourceProcess.getSourceElements().contains( sink ) )
            addEdge( sinkProcess, sourceProcess, new ProcessEdge<>( sourceProcess, (FlowElement) sink, sinkProcess ) );
          }
        }
      }
    }

  public Set<FlowElement> getSourceElements()
    {
    return sourceElements;
    }

  public Set<FlowElement> getSinkElements()
    {
    return sinkElements;
    }

  public Set<Tap> getSourceTaps()
    {
    if( sourceTaps != null )
      return sourceTaps;

    sourceTaps = Util.narrowSet( Tap.class, getSourceElements() );

    return sourceTaps;
    }

  public Set<Tap> getSinkTaps()
    {
    if( sinkTaps != null )
      return sinkTaps;

    sinkTaps = Util.narrowSet( Tap.class, getSinkElements() );

    return sinkTaps;
    }

  public Map<String, Tap> getTrapsMap()
    {
    return trapsMap;
    }

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

  public Iterator<Process> getOrderedTopologicalIterator( Comparator<Process> comparator )
    {
    return new TopologicalOrderIterator<>( this, new PriorityQueue<>( 10, comparator ) );
    }

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

  /**
   * Method writeDOT writes this element graph to a DOT file for easy visualization and debugging.
   *
   * @param filename of type String
   */
  public void writeDOT( String filename )
    {
    printProcessGraph( filename );
    }

  protected void printProcessGraph( String filename )
    {
    try
      {
      Writer writer = new FileWriter( filename );

      Util.writeDOT( writer, this, new IntegerNameProvider<Process>(), new VertexNameProvider<Process>()
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

  public static class ProcessEdge<Process extends ProcessModel>
    {
    FlowElement flowElement;
    Set<Integer> outgoingOrdinals; // ordinals entering this edge exiting the source process
    Set<Integer> incomingOrdinals; // ordinals exiting the edge into the sink process
    Set<Enum> sinkAnnotations = Collections.emptySet();
    Set<Enum> sourceAnnotations = Collections.emptySet();

    public ProcessEdge( Process sourceProcess, FlowElement flowElement, Process sinkProcess )
      {
      this.flowElement = flowElement;

      ElementGraph sinkElementGraph = sinkProcess.getElementGraph();
      ElementGraph sourceElementGraph = sourceProcess.getElementGraph();

      this.incomingOrdinals = createOrdinals( sinkElementGraph.incomingEdgesOf( flowElement ) );
      this.outgoingOrdinals = createOrdinals( sourceElementGraph.outgoingEdgesOf( flowElement ) );

      if( sinkElementGraph instanceof AnnotatedGraph && ( (AnnotatedGraph) sinkElementGraph ).hasAnnotations() )
        this.sinkAnnotations = ( (AnnotatedGraph) sinkElementGraph ).getAnnotations().getKeysFor( flowElement );

      if( sourceElementGraph instanceof AnnotatedGraph && ( (AnnotatedGraph) sourceElementGraph ).hasAnnotations() )
        this.sourceAnnotations = ( (AnnotatedGraph) sourceElementGraph ).getAnnotations().getKeysFor( flowElement );
      }

    private Set<Integer> createOrdinals( Set<Scope> scopes )
      {
      Set<Integer> ordinals = new HashSet<>();

      for( Scope scope : scopes )
        ordinals.add( scope.getOrdinal() );

      return ordinals;
      }

    public FlowElement getFlowElement()
      {
      return flowElement;
      }

    public String getID()
      {
      return FlowElements.id( flowElement );
      }

    public Set<Integer> getIncomingOrdinals()
      {
      return incomingOrdinals;
      }

    public Set<Integer> getOutgoingOrdinals()
      {
      return outgoingOrdinals;
      }

    public Set<Enum> getSinkAnnotations()
      {
      return sinkAnnotations;
      }

    public Set<Enum> getSourceAnnotations()
      {
      return sourceAnnotations;
      }
    }
  }
