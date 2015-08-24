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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.util.Util;

/**
 *
 */
public class DecoratedElementGraph implements ElementGraph
  {
  ElementGraph decorated;

  public DecoratedElementGraph( ElementGraph decorated )
    {
    this.decorated = decorated;
    }

  public ElementGraph getDecorated()
    {
    return decorated;
    }

  @Override
  public ElementGraph copyElementGraph()
    {
    return decorated.copyElementGraph();
    }

  @Override
  public void writeDOT( String filename )
    {
    boolean success = ElementGraphs.printElementGraph( filename, this, null );

    if( success )
      Util.writePDF( filename );
    }

  @Override
  public int inDegreeOf( FlowElement vertex )
    {
    return decorated.inDegreeOf( vertex );
    }

  @Override
  public Set<Scope> incomingEdgesOf( FlowElement vertex )
    {
    return decorated.incomingEdgesOf( vertex );
    }

  @Override
  public int outDegreeOf( FlowElement vertex )
    {
    return decorated.outDegreeOf( vertex );
    }

  @Override
  public Set<Scope> outgoingEdgesOf( FlowElement vertex )
    {
    return decorated.outgoingEdgesOf( vertex );
    }

  @Override
  public List<FlowElement> predecessorListOf( FlowElement flowElement )
    {
    return decorated.predecessorListOf( flowElement );
    }

  @Override
  public List<FlowElement> successorListOf( FlowElement flowElement )
    {
    return decorated.successorListOf( flowElement );
    }

  @Override
  public Set<Scope> getAllEdges( FlowElement sourceVertex, FlowElement targetVertex )
    {
    return decorated.getAllEdges( sourceVertex, targetVertex );
    }

  @Override
  public Scope getEdge( FlowElement sourceVertex, FlowElement targetVertex )
    {
    return decorated.getEdge( sourceVertex, targetVertex );
    }

  @Override
  public Scope addEdge( FlowElement sourceVertex, FlowElement targetVertex )
    {
    return decorated.addEdge( sourceVertex, targetVertex );
    }

  @Override
  public boolean addEdge( FlowElement sourceVertex, FlowElement targetVertex, Scope scope )
    {
    return decorated.addEdge( sourceVertex, targetVertex, scope );
    }

  @Override
  public boolean addHeadVertex( FlowElement flowElement )
    {
    return decorated.addHeadVertex( flowElement );
    }

  @Override
  public boolean addTailVertex( FlowElement flowElement )
    {
    return decorated.addTailVertex( flowElement );
    }

  @Override
  public boolean addVertex( FlowElement flowElement )
    {
    return decorated.addVertex( flowElement );
    }

  @Override
  public boolean containsEdge( FlowElement sourceVertex, FlowElement targetVertex )
    {
    return decorated.containsEdge( sourceVertex, targetVertex );
    }

  @Override
  public boolean containsEdge( Scope scope )
    {
    return decorated.containsEdge( scope );
    }

  @Override
  public boolean containsVertex( FlowElement flowElement )
    {
    return decorated.containsVertex( flowElement );
    }

  @Override
  public Set<Scope> edgeSet()
    {
    return decorated.edgeSet();
    }

  @Override
  public Set<Scope> edgesOf( FlowElement vertex )
    {
    return decorated.edgesOf( vertex );
    }

  @Override
  public boolean removeAllEdges( Collection<? extends Scope> edges )
    {
    return decorated.removeAllEdges( edges );
    }

  @Override
  public Set<Scope> removeAllEdges( FlowElement sourceVertex, FlowElement targetVertex )
    {
    return decorated.removeAllEdges( sourceVertex, targetVertex );
    }

  @Override
  public boolean removeAllVertices( Collection<? extends FlowElement> vertices )
    {
    return decorated.removeAllVertices( vertices );
    }

  @Override
  public Scope removeEdge( FlowElement sourceVertex, FlowElement targetVertex )
    {
    return decorated.removeEdge( sourceVertex, targetVertex );
    }

  @Override
  public boolean removeEdge( Scope scope )
    {
    return decorated.removeEdge( scope );
    }

  @Override
  public boolean removeVertex( FlowElement flowElement )
    {
    return decorated.removeVertex( flowElement );
    }

  @Override
  public Set<FlowElement> vertexSet()
    {
    return decorated.vertexSet();
    }

  @Override
  public FlowElement getEdgeSource( Scope scope )
    {
    return decorated.getEdgeSource( scope );
    }

  @Override
  public FlowElement getEdgeTarget( Scope scope )
    {
    return decorated.getEdgeTarget( scope );
    }

  @Override
  public boolean equals( Object object )
    {
    return ElementGraphs.equals( this, (ElementGraph) object );
    }

  @Override
  public int hashCode()
    {
    int result = decorated.hashCode();
    result = 31 * result; // parity with AnnotatedGraph types
    return result;
    }
  }
