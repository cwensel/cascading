/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.stream.graph;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Set;

import cascading.flow.stream.duct.CloseReducingDuct;
import cascading.flow.stream.duct.Collapsing;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.DuctGraph;
import cascading.flow.stream.duct.Fork;
import cascading.flow.stream.duct.Gate;
import cascading.flow.stream.duct.OpenDuct;
import cascading.flow.stream.duct.OpenReducingDuct;
import cascading.flow.stream.duct.OpenWindow;
import cascading.flow.stream.duct.OrdinalDuct;
import cascading.flow.stream.duct.Reducing;
import cascading.flow.stream.duct.Stage;
import cascading.flow.stream.duct.Window;
import cascading.util.Util;
import org.jgrapht.DirectedGraph;
import org.jgrapht.EdgeFactory;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class StreamGraph is the operation pipeline used during processing. This an internal use only class.
 * <p/>
 * Under some circumstances it may make sense to see the actual graph plan. To do so, enable one or both dot file
 * properties, {@link #ERROR_DOT_FILE_NAME} and {@link #DOT_FILE_PATH}.
 */
public class StreamGraph
  {
  /** Property denoting the path and filename to write the failed stream graph dot file. */
  public final static String ERROR_DOT_FILE_NAME = "cascading.stream.error.dotfile";

  /**
   * Property denoting the path to write all stream graph dot files. The filename will be generated
   * based on platform properties.
   */
  public final static String DOT_FILE_PATH = "cascading.stream.dotfile.path";

  private static final Logger LOG = LoggerFactory.getLogger( StreamGraph.class );

  private final Duct HEAD = new Extent( "head" );
  private final Duct TAIL = new Extent( "tail" );

  private final DuctGraph ductGraph = new DuctGraph();

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
    addPath( lhs, 0, rhs );
    }

  public void addPath( Duct lhs, int ordinal, Duct rhs )
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
      ductGraph.addVertex( lhs );
      ductGraph.addVertex( rhs );
      ductGraph.addEdge( lhs, rhs, ductGraph.makeOrdinal( ordinal ) );
      }
    catch( RuntimeException exception )
      {
      LOG.error( "unable to add path", exception );
      printGraphError();
      throw exception;
      }
    }

  public void bind()
    {
    Iterator<Duct> iterator = getTopologicalOrderIterator();

    // build the actual processing graph
    while( iterator.hasNext() )
      iterator.next().bind( this );

    iterator = getReversedTopologicalOrderIterator();

    // initialize all the ducts
    while( iterator.hasNext() )
      iterator.next().initialize();
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
    return Graphs.successorListOf( ductGraph, getHEAD() );
    }

  public Collection<Duct> getTails()
    {
    return Graphs.predecessorListOf( ductGraph, getTAIL() );
    }

  public Duct[] findAllNextFor( Duct current )
    {
    Set<DuctGraph.Ordinal> outgoing = ductGraph.outgoingEdgesOf( current );
    LinkedList<Duct> successors = new LinkedList<Duct>();

    for( DuctGraph.Ordinal edge : outgoing )
      {
      Duct successor = ductGraph.getEdgeTarget( edge );

      if( successor == getHEAD() )
        throw new IllegalStateException( "HEAD may not be next" );

      if( successor == getTAIL() ) // tail is not included, its just a marker
        continue;

      successor = wrapWithOrdinal( edge, successor );

      successors.add( successor );
      }

    return successors.toArray( new Duct[ successors.size() ] );
    }

  public Duct[] findAllPreviousFor( Duct current )
    {
    LinkedList<Duct> predecessors = new LinkedList<Duct>( Graphs.predecessorListOf( ductGraph, current ) );
    ListIterator<Duct> iterator = predecessors.listIterator();

    while( iterator.hasNext() )
      {
      Duct successor = iterator.next();

      if( successor == getTAIL() )
        throw new IllegalStateException( "TAIL may not be successor" );

      if( successor == getHEAD() ) // head is not included, its just a marker
        iterator.remove();
      }

    return predecessors.toArray( new Duct[ predecessors.size() ] );
    }

  public Duct createNextFor( Duct current )
    {
    if( current == getHEAD() || current == getTAIL() )
      return null;

    Set<DuctGraph.Ordinal> edges = ductGraph.outgoingEdgesOf( current );

    if( edges.size() == 0 )
      throw new IllegalStateException( "ducts must have an outgoing edge, current: " + current );

    DuctGraph.Ordinal edge = edges.iterator().next();
    Duct next = ductGraph.getEdgeTarget( edge );

    if( current instanceof Gate )
      {
      if( !( current instanceof Window ) ) // just collapse - Merge
        {
        if( edges.size() > 1 )
          return createFork( findAllNextFor( current ) );

        return wrapWithOrdinal( edge, next );
        }

      if( next instanceof OpenWindow )
        return next;

      if( edges.size() > 1 )
        return createOpenWindow( createFork( findAllNextFor( current ) ) );

      if( next instanceof Reducing )
        return createOpenReducingWindow( next );

      return createOpenWindow( wrapWithOrdinal( edge, next ) );
      }

    if( current instanceof Reducing )
      {
      if( next instanceof Reducing )
        return next;

      if( edges.size() > 1 )
        return createCloseWindow( createFork( findAllNextFor( current ) ) );

      return createCloseWindow( wrapWithOrdinal( edge, next ) );
      }

    if( edges.size() > 1 )
      return createFork( findAllNextFor( current ) );

    if( next == getTAIL() ) // tail is not included, its just a marker
      throw new IllegalStateException( "tail ducts should not bind to next" );

    return wrapWithOrdinal( edge, next );
    }

  protected Duct wrapWithOrdinal( DuctGraph.Ordinal edge, Duct next )
    {
    if( next instanceof Collapsing )
      next = new OrdinalDuct( next, edge.getOrdinal() );

    return next;
    }

  protected Duct createCloseWindow( Duct next )
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

  public int ordinalBetween( Duct lhs, Duct rhs )
    {
    return ductGraph.getEdge( lhs, rhs ).getOrdinal();
    }

  public TopologicalOrderIterator<Duct, Integer> getTopologicalOrderIterator()
    {
    try
      {
      return new TopologicalOrderIterator( ductGraph );
      }
    catch( RuntimeException exception )
      {
      LOG.error( "failed creating topological iterator", exception );
      printGraphError();

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
      printGraphError();

      throw exception;
      }
    }

  public DirectedGraph getReversedGraph()
    {
    DuctGraph reversedGraph = new DuctGraph();

    Graphs.addGraphReversed( reversedGraph, ductGraph );

    return reversedGraph;
    }

  public Collection<Duct> getAllDucts()
    {
    return ductGraph.vertexSet();
    }

  public void printGraphError()
    {
    String filename = (String) getProperty( ERROR_DOT_FILE_NAME );

    if( filename == null )
      return;

    printGraph( filename );
    }

  public void printGraph( String id, String classifier, int discriminator )
    {
    String path = (String) getProperty( DOT_FILE_PATH );

    if( path == null )
      return;

    classifier = Util.cleansePathName( classifier );

    path = String.format( "%s/streamgraph-%s-%s-%s.dot", path, id, classifier, discriminator );

    printGraph( path );
    }

  public void printGraph( String filename )
    {
    LOG.info( "writing stream graph to {}", filename );
    Util.printGraph( filename, ductGraph );
    }

  public void printBoundGraph( String id, String classifier, int discriminator )
    {
    String path = (String) getProperty( DOT_FILE_PATH );

    if( path == null )
      return;

    classifier = Util.cleansePathName( classifier );

    path = String.format( "%s/streamgraph-bound-%s-%s-%s.dot", path, id, classifier, discriminator );

    printBoundGraph( path );
    }

  public void printBoundGraph( String filename )
    {
    LOG.info( "writing stream bound graph to {}", filename );

    DirectedMultigraph<Duct, Integer> graph = new DirectedMultigraph<>( new EdgeFactory<Duct, Integer>()
      {
      int count = 0;

      @Override
      public Integer createEdge( Duct sourceVertex, Duct targetVertex )
        {
        return count++;
        }
      } );

    TopologicalOrderIterator<Duct, Integer> iterator = getTopologicalOrderIterator();

    while( iterator.hasNext() )
      {
      Duct previous = iterator.next();

      if( graph.containsVertex( previous ) || previous instanceof Extent )
        continue;

      graph.addVertex( previous );
      addNext( graph, previous );
      }

    Util.printGraph( filename, graph );
    }

  private void addNext( DirectedMultigraph graph, Duct previous )
    {

    if( previous instanceof Fork )
      {
      for( Duct next : ( (Fork) previous ).getAllNext() )
        {
        if( next == null || next instanceof Extent )
          continue;

        graph.addVertex( next );

        if( graph.containsEdge( previous, next ) )
          continue;

        graph.addEdge( previous, next );

        addNext( graph, next );
        }
      }
    else
      {
      Duct next = previous.getNext();

      if( next == null || next instanceof Extent )
        return;

      graph.addVertex( next );

      if( graph.containsEdge( previous, next ) )
        return;

      graph.addEdge( previous, next );

      addNext( graph, next );
      }
    }
  }