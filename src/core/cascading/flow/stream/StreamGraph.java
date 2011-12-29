/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import cascading.util.Util;
import org.jgrapht.DirectedGraph;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.KShortestPaths;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StreamGraph
  {
  public final static String ERROR_DOT_FILE_PATH = "cascading.stream.error.dotfile";

  private static final Logger LOG = LoggerFactory.getLogger( StreamGraph.class );

  private final Duct HEAD = new Extent( "head" );
  private final Duct TAIL = new Extent( "tail" );

  private final DuctGraph graph = new DuctGraph();

  private class Extent extends Stage
    {
    final String name;

    private Extent( String name )
      {
      this.name = name;
      }

    @Override
    public String toString()
      {
      return name;
      }
    }

  public StreamGraph()
    {
    }

  protected Object getProperty( String name )
    {
    return null;
    }

  Duct getHEAD()
    {
    return HEAD;
    }

  Duct getTAIL()
    {
    return TAIL;
    }

  public void addHead( Duct head )
    {
    addPath( getHEAD(), head );
    }

  public void addTail( Duct tail )
    {
    addPath( tail, getTAIL() );
    }

  public void addPath( Duct lhs, Duct rhs )
    {
    if( lhs == null && rhs == null )
      throw new IllegalArgumentException( "both lhs and rhs may not be null" );

    if( lhs == getTAIL() )
      throw new IllegalStateException( "lhs may not be a TAIL" );

    if( rhs == getHEAD() )
      throw new IllegalStateException( "rhs may not be a HEAD" );

    if( lhs == null )
      lhs = getHEAD();

    if( rhs == null )
      rhs = getTAIL();

    try
      {
      graph.addVertex( lhs );
      graph.addVertex( rhs );
      graph.addEdge( lhs, rhs );
      }
    catch( RuntimeException exception )
      {
      LOG.error( "unable to add path", exception );
      printGraph();
      throw exception;
      }
    }

  public void bind()
    {
    Iterator<Duct> iterator = getTopologicalOrderIterator();

    // build the actual processing graph
    while( iterator.hasNext() )
      iterator.next().bind( this );

    // initialize all the ducts
    for( Duct duct : getAllDucts() )
      duct.initialize();
    }

  /** Calls prepare starting at the tail and working backwards */
  public void prepare()
    {
    TopologicalOrderIterator<Duct, Integer> iterator = getReversedTopologicalOrderIterator();

    while( iterator.hasNext() )
      iterator.next().prepare();
    }

  /** Calls cleanup starting at the head and working forwards */
  public void cleanup()
    {
    TopologicalOrderIterator<Duct, Integer> iterator = getTopologicalOrderIterator();

    while( iterator.hasNext() )
      iterator.next().cleanup();
    }

  public Collection<Duct> getHeads()
    {
    return Graphs.successorListOf( graph, getHEAD() );
    }

  public Collection<Duct> getTails()
    {
    return Graphs.predecessorListOf( graph, getTAIL() );
    }

  public Duct[] findAllNextFor( Duct current )
    {
    LinkedList<Duct> successors = new LinkedList<Duct>( Graphs.successorListOf( graph, current ) );
    ListIterator<Duct> iterator = successors.listIterator();

    while( iterator.hasNext() )
      {
      Duct successor = iterator.next();

      if( successor == getHEAD() )
        throw new IllegalStateException( "HEAD may not be next" );

      if( successor == getTAIL() ) // tail is not included, its just a marker
        iterator.remove();
      }

    return successors.toArray( new Duct[ successors.size() ] );
    }

  public Duct[] findAllPreviousFor( Duct current )
    {
    LinkedList<Duct> predecessors = new LinkedList<Duct>( Graphs.predecessorListOf( graph, current ) );
    ListIterator<Duct> iterator = predecessors.listIterator();

    while( iterator.hasNext() )
      {
      Duct successor = iterator.next();

      if( successor == getTAIL() )
        throw new IllegalStateException( "TAIL may not be next" );

      if( successor == getHEAD() ) // head is not included, its just a marker
        iterator.remove();
      }

    return predecessors.toArray( new Duct[ predecessors.size() ] );
    }

  public Duct createNextFor( Duct current )
    {
    if( current == getHEAD() || current == getTAIL() )
      return null;

    Set<Integer> edges = graph.outgoingEdgesOf( current );

    if( edges.size() == 0 )
      throw new IllegalStateException( "ducts must have an outgoing edge" );

    Duct next = graph.getEdgeTarget( edges.iterator().next() );

    if( current instanceof Gate )
      {
      if( next instanceof OpenWindow )
        return next;

      if( edges.size() > 1 )
        return createOpenWindow( createFork( findAllNextFor( current ) ) );

      if( next instanceof Reducing )
        return createOpenReducingWindow( next );

      return createOpenWindow( next );
      }

    if( current instanceof Reducing )
      {
      if( next instanceof Reducing )
        return next;

      if( edges.size() > 1 )
        return createCloseWindow( createFork( findAllNextFor( current ) ) );

      return createCloseWindow( next );
      }

    if( edges.size() > 1 )
      return createFork( findAllNextFor( current ) );

    if( next == getTAIL() ) // tail is not included, its just a marker
      throw new IllegalStateException( "tail ducts should not bind to next" );

    return next;
    }

  private Duct createCloseWindow( Duct next )
    {
    return new CloseReducingDuct( next );
    }

  protected Duct createOpenWindow( Duct next )
    {
    return new OpenDuct( next );
    }

  protected Duct createOpenReducingWindow( Duct next )
    {
    return new OpenReducingDuct( next );
    }

  protected Duct createFork( Duct[] allNext )
    {
    return new Fork( allNext );
    }

  /**
   * Returns all free paths to the current duct, usually a GroupGate.
   * <p/>
   * Paths all unique paths are counted, minus any immediate prior GroupGates as they
   * block incoming paths into a single path
   *
   * @param duct
   * @return
   */
  public int countAllIncomingPathsTo( Duct duct )
    {
    // find all immediate prior groups/gates
    LinkedList<List<Duct>> paths = asPathList( allPathsBetweenInclusive( getHEAD(), duct ) );

    Set<Duct> gates = new HashSet<Duct>();

    for( List<Duct> path : paths )
      {
      Collections.reverse( path );

      path.remove( 0 ); // remove the duct param

      for( Duct element : path )
        {
        if( !( element instanceof Gate ) )
          continue;

        gates.add( element );
        break;
        }
      }

    // find all paths
    // remove all paths containing prior groups
    ListIterator<List<Duct>> iterator = paths.listIterator();
    while( iterator.hasNext() )
      {
      List<Duct> path = iterator.next();

      if( !Collections.disjoint( path, gates ) )
        iterator.remove();
      }

    // incoming == paths + prior groups
    return paths.size() + gates.size();
    }

  private List<GraphPath<Duct, Integer>> allPathsBetweenInclusive( Duct from, Duct to )
    {
    return new KShortestPaths<Duct, Integer>( graph, from, Integer.MAX_VALUE ).getPaths( to );
    }

  public static LinkedList<List<Duct>> asPathList( List<GraphPath<Duct, Integer>> paths )
    {
    LinkedList<List<Duct>> results = new LinkedList<List<Duct>>();

    if( paths == null )
      return results;

    for( GraphPath<Duct, Integer> path : paths )
      results.add( Graphs.getPathVertexList( path ) );

    return results;
    }

  public TopologicalOrderIterator<Duct, Integer> getTopologicalOrderIterator()
    {
    try
      {
      return new TopologicalOrderIterator( graph );
      }
    catch( RuntimeException exception )
      {
      LOG.error( "failed creating topological iterator", exception );
      printGraph();

      throw exception;
      }
    }

  public TopologicalOrderIterator<Duct, Integer> getReversedTopologicalOrderIterator()
    {
    try
      {
      return new TopologicalOrderIterator( getReversedGraph() );
      }
    catch( RuntimeException exception )
      {
      LOG.error( "failed creating reversed topological iterator", exception );
      printGraph();

      throw exception;
      }
    }

  public DirectedGraph getReversedGraph()
    {
    DuctGraph reversedGraph = new DuctGraph();

    Graphs.addGraphReversed( reversedGraph, graph );

    return reversedGraph;
    }

  public Collection<Duct> getAllDucts()
    {
    return graph.vertexSet();
    }

  public void printGraph()
    {
    String filename = (String) getProperty( ERROR_DOT_FILE_PATH );

    if( filename == null )
      return;

    printGraph( filename );
    }

  public void printGraph( String filename )
    {
    LOG.info( "writing stream graph to {}", filename );
    Util.printGraph( filename, graph );
    }
  }