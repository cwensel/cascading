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

package cascading.flow.planner.graph;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.util.Util;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graphs;

/**
 *
 */
public abstract class BaseElementGraph implements ElementGraph, Serializable
  {
  protected DirectedGraph<FlowElement, Scope> graph;

  public BaseElementGraph()
    {
    }

  public BaseElementGraph( DirectedGraph<FlowElement, Scope> graph )
    {
    this.graph = graph;
    }

  public boolean containsEdge( FlowElement sourceVertex, FlowElement targetVertex )
    {
    return graph.containsEdge( sourceVertex, targetVertex );
    }

  public boolean removeAllEdges( Collection<? extends Scope> edges )
    {
    return graph.removeAllEdges( edges );
    }

  public Set<Scope> removeAllEdges( FlowElement sourceVertex, FlowElement targetVertex )
    {
    return graph.removeAllEdges( sourceVertex, targetVertex );
    }

  public boolean removeAllVertices( Collection<? extends FlowElement> vertices )
    {
    return graph.removeAllVertices( vertices );
    }

  public Set<Scope> getAllEdges( FlowElement sourceVertex, FlowElement targetVertex )
    {
    return graph.getAllEdges( sourceVertex, targetVertex );
    }

  public Scope getEdge( FlowElement sourceVertex, FlowElement targetVertex )
    {
    return graph.getEdge( sourceVertex, targetVertex );
    }

  public Scope addEdge( FlowElement sourceVertex, FlowElement targetVertex )
    {
    return graph.addEdge( sourceVertex, targetVertex );
    }

  public boolean addEdge( FlowElement sourceVertex, FlowElement targetVertex, Scope scope )
    {
    return graph.addEdge( sourceVertex, targetVertex, scope );
    }

  public boolean addVertex( FlowElement flowElement )
    {
    return graph.addVertex( flowElement );
    }

  public FlowElement getEdgeSource( Scope scope )
    {
    return graph.getEdgeSource( scope );
    }

  public FlowElement getEdgeTarget( Scope scope )
    {
    return graph.getEdgeTarget( scope );
    }

  public boolean containsEdge( Scope scope )
    {
    return graph.containsEdge( scope );
    }

  public boolean containsVertex( FlowElement flowElement )
    {
    return graph.containsVertex( flowElement );
    }

  public Set<Scope> edgeSet()
    {
    return graph.edgeSet();
    }

  public Set<Scope> edgesOf( FlowElement vertex )
    {
    return graph.edgesOf( vertex );
    }

  public int inDegreeOf( FlowElement vertex )
    {
    return graph.inDegreeOf( vertex );
    }

  public Set<Scope> incomingEdgesOf( FlowElement vertex )
    {
    return graph.incomingEdgesOf( vertex );
    }

  public int outDegreeOf( FlowElement vertex )
    {
    return graph.outDegreeOf( vertex );
    }

  public Set<Scope> outgoingEdgesOf( FlowElement vertex )
    {
    return graph.outgoingEdgesOf( vertex );
    }

  public Scope removeEdge( FlowElement sourceVertex, FlowElement targetVertex )
    {
    return graph.removeEdge( sourceVertex, targetVertex );
    }

  public boolean removeEdge( Scope scope )
    {
    return graph.removeEdge( scope );
    }

  public boolean removeVertex( FlowElement flowElement )
    {
    return graph.removeVertex( flowElement );
    }

  public Set<FlowElement> vertexSet()
    {
    return graph.vertexSet();
    }

  @Override
  public List<FlowElement> predecessorListOf( FlowElement flowElement )
    {
    return Graphs.predecessorListOf( graph, flowElement );
    }

  @Override
  public List<FlowElement> successorListOf( FlowElement flowElement )
    {
    return Graphs.successorListOf( graph, flowElement );
    }

  @Override
  public void writeDOT( String filename )
    {
    boolean success = ElementGraphs.printElementGraph( filename, this, null );

    if( success )
      Util.writePDF( filename );
    }

  @Override
  public boolean equals( Object object )
    {
    return ElementGraphs.equals( this, (ElementGraph) object );
    }

  @Override
  public int hashCode()
    {
    int result = graph.hashCode();
    result = 31 * result; // parity with AnnotatedGraph types
    return result;
    }
  }
