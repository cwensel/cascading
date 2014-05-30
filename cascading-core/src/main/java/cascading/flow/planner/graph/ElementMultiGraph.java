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

package cascading.flow.planner.graph;

import cascading.flow.FlowElement;
import cascading.flow.planner.ElementGraphs;
import cascading.flow.planner.Scope;
import cascading.util.EnumMultiMap;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DirectedMultigraph;

/**
 *
 */
public class ElementMultiGraph extends DirectedMultigraph<FlowElement, Scope> implements ElementGraph, AnnotatedGraph
  {
  protected EnumMultiMap annotations;

  public ElementMultiGraph( DirectedGraph<FlowElement, Scope> parent )
    {
    this();

    copyFrom( parent );

    if( parent instanceof AnnotatedGraph && ( ( (AnnotatedGraph) parent ) ).hasAnnotations() )
      this.getAnnotations().addAll( ( (AnnotatedGraph) parent ).getAnnotations() );
    }

  protected void copyFrom( DirectedGraph<FlowElement, Scope> parent )
    {
    // safe to assume there are no unconnected vertices
    for( Scope edge : parent.edgeSet() )
      {
      FlowElement s = parent.getEdgeSource( edge );
      FlowElement t = parent.getEdgeTarget( edge );
      addVertex( s );
      addVertex( t );
      addEdge( s, t, edge );
      }
    }

  public ElementMultiGraph( ElementGraph parent, EnumMultiMap annotations )
    {
    this( parent );

    this.getAnnotations().addAll( annotations );
    }

  public ElementMultiGraph()
    {
    super( Scope.class );
    }

  @Override
  public ElementGraph copyGraph()
    {
    return new ElementMultiGraph( this );
    }

  @Override
  public void writeDOT( String filename )
    {
    ElementGraphs.printElementGraph( filename, this, null );
    }

  @Override
  public boolean hasAnnotations()
    {
    return annotations != null && !annotations.isEmpty();
    }

  @Override
  public EnumMultiMap getAnnotations()
    {
    if( annotations == null )
      annotations = new EnumMultiMap();

    return annotations;
    }
  }
