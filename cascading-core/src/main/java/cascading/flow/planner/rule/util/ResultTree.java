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

package cascading.flow.planner.rule.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import cascading.flow.planner.graph.ElementGraph;
import cascading.util.Util;
import org.jgrapht.EdgeFactory;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;

/**
 *
 */
public class ResultTree
  {
  private final SimpleDirectedGraph<Delegate, Path> graph;

  public static class Delegate
    {
    ElementGraph graph;

    public Delegate( ElementGraph graph )
      {
      this.graph = graph;
      }

    @Override
    public boolean equals( Object object )
      {
      if( this == object )
        return true;

      Delegate delegate = (Delegate) object;

      return graph == delegate.graph;
      }

    @Override
    public int hashCode()
      {
      return System.identityHashCode( graph );
      }
    }

  public static class Path
    {
    int[] ordinals;

    public Path( Path prior, int ordinal )
      {
      int[] priorOrdinals = prior == null ? new int[]{} : prior.ordinals;

      ordinals = new int[ priorOrdinals.length + 1 ];
      System.arraycopy( priorOrdinals, 0, ordinals, 0, priorOrdinals.length );
      ordinals[ priorOrdinals.length ] = ordinal;
      }

    public Path( int... ordinals )
      {
      this.ordinals = ordinals;
      }

    public int[] getOrdinals()
      {
      return ordinals;
      }
    }

  private static class PathFactory implements EdgeFactory<Delegate, Path>
    {
    ResultTree tree;

    private PathFactory()
      {
      }

    @Override
    public Path createEdge( Delegate sourceVertex, Delegate targetVertex )
      {
      Set<Path> paths = tree.graph.incomingEdgesOf( sourceVertex );

      if( paths.size() > 1 )
        throw new IllegalStateException( "too many incoming edges" );

      Path path = Util.getFirst( paths );

      return new Path( path, tree.graph.outDegreeOf( sourceVertex ) );
      }
    }

  public ResultTree()
    {
    graph = new SimpleDirectedGraph<>( new PathFactory() );

    ( (PathFactory) graph.getEdgeFactory() ).tree = this;
    }

  public void setChildren( ElementGraph parent, List<? extends ElementGraph> children )
    {
    Delegate parentDelegate = new Delegate( parent );

    if( !graph.addVertex( parentDelegate ) )
      graph.removeAllVertices( Graphs.successorListOf( graph, parentDelegate ) );

    for( ElementGraph child : children )
      {
      Delegate childDelegate = new Delegate( child );
      graph.addVertex( childDelegate );
      graph.addEdge( parentDelegate, childDelegate );
      }
    }

  public List<? extends ElementGraph> getChildren( ElementGraph parent )
    {
    List<Delegate> delegates = Graphs.successorListOf( graph, new Delegate( parent ) );
    List<ElementGraph> results = new ArrayList<>();

    for( Delegate delegate : delegates )
      results.add( delegate.graph );

    return results;
    }

  public Path getEdge( ElementGraph parent, ElementGraph child )
    {
    return graph.getEdge( new Delegate( parent ), new Delegate( child ) );
    }

  public Path getIncomingEdge( ElementGraph parent )
    {
    return Util.getFirst( graph.incomingEdgesOf( new Delegate( parent ) ) );
    }
  }
