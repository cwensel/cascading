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

package cascading.flow.planner;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementMaskSubGraph;
import cascading.flow.planner.graph.ElementSubGraph;
import cascading.flow.planner.iso.expression.ElementExpression;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.flow.planner.iso.subgraph.SubGraphIterator;
import cascading.flow.stream.StreamMode;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.Splice;
import cascading.tap.Tap;
import cascading.util.Util;
import cascading.util.Version;
import org.jgrapht.DirectedGraph;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.BiconnectivityInspector;
import org.jgrapht.alg.FloydWarshallShortestPaths;
import org.jgrapht.alg.KShortestPaths;
import org.jgrapht.ext.ComponentAttributeProvider;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.graph.SimpleGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.Util.getFirst;
import static cascading.util.Util.narrowSet;
import static java.lang.Double.POSITIVE_INFINITY;

/**
 *
 */
public class ElementGraphs
  {
  private static final Logger LOG = LoggerFactory.getLogger( ElementGraphs.class );

  public static TopologicalOrderIterator<FlowElement, Scope> getTopologicalIterator( ElementGraph graph )
    {
    return new TopologicalOrderIterator<>( graph );
    }

  /**
   * Method getAllShortestPathsBetween ...
   *
   * @param graph
   * @param from  of type FlowElement
   * @param to    of type FlowElement
   * @return List<GraphPath<FlowElement, Scope>>
   */
  public static <V, E> List<GraphPath<V, E>> getAllShortestPathsBetween( DirectedGraph<V, E> graph, V from, V to )
    {
    List<GraphPath<V, E>> paths = new KShortestPaths<>( graph, from, Integer.MAX_VALUE ).getPaths( to );

    if( paths == null )
      return new ArrayList<>();

    return paths;
    }

  /**
   * All paths that lead from to to without crossing a Tap/Group boundary
   *
   * @param graph
   * @param from
   * @param to
   * @return of type List
   */
  public static List<GraphPath<FlowElement, Scope>> getAllDirectPathsBetween( ElementGraph graph, FlowElement from, FlowElement to )
    {
    List<GraphPath<FlowElement, Scope>> paths = getAllShortestPathsBetween( graph, from, to );
    List<GraphPath<FlowElement, Scope>> results = new ArrayList<>( paths );

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      List<FlowElement> pathVertexList = Graphs.getPathVertexList( path );

      for( int i = 1; i < pathVertexList.size(); i++ ) // skip the from, its a Tap or Group
        {
        FlowElement flowElement = pathVertexList.get( i );

        if( flowElement instanceof Tap || flowElement instanceof Group )
          {
          results.remove( path );
          break;
          }
        }
      }

    return results;
    }

  /**
   * for every incoming stream to the splice, gets the count of paths.
   * <p/>
   * covers the case where a source may cross multiple joins to the current join and still land
   * on the lhs or rhs.
   *
   * @param graph
   * @param from
   * @param to
   * @return of type Map
   */
  public static Map<Integer, Integer> countOrderedDirectPathsBetween( ElementGraph graph, FlowElement from, Splice to )
    {
    return countOrderedDirectPathsBetween( graph, from, to, false );
    }

  public static Map<Integer, Integer> countOrderedDirectPathsBetween( ElementGraph graph, FlowElement from, Splice to, boolean skipTaps )
    {
    List<GraphPath<FlowElement, Scope>> paths = getAllDirectPathsBetween( graph, from, to );

    Map<Integer, Integer> results = new HashMap<Integer, Integer>();

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      if( skipTaps && hasIntermediateTap( path, from ) )
        continue;

      pathPositionInto( results, path, to );
      }

    return results;
    }

  public static boolean isBothAccumulatedAndStreamedPath( Map<Integer, Integer> pathCounts )
    {
    return pathCounts.size() > 1 && pathCounts.containsKey( 0 );
    }

  public static boolean isOnlyStreamedPath( Map<Integer, Integer> pathCounts )
    {
    return pathCounts.size() == 1 && pathCounts.containsKey( 0 );
    }

  public static boolean isOnlyAccumulatedPath( Map<Integer, Integer> pathCounts )
    {
    return pathCounts.size() >= 1 && !pathCounts.containsKey( 0 );
    }

  private static boolean hasIntermediateTap( GraphPath<FlowElement, Scope> path, FlowElement from )
    {
    List<FlowElement> flowElements = Graphs.getPathVertexList( path );

    for( FlowElement flowElement : flowElements )
      {
      if( flowElement instanceof Tap && flowElement != from )
        return true;
      }

    return false;
    }

  private static Map<Integer, Integer> pathPositionInto( Map<Integer, Integer> results, GraphPath<FlowElement, Scope> path, Splice to )
    {
    List<Scope> scopes = path.getEdgeList();

    Scope lastScope = scopes.get( scopes.size() - 1 );

    Integer pos = to.getPipePos().get( lastScope.getName() );

    if( results.containsKey( pos ) )
      results.put( pos, results.get( pos ) + 1 );
    else
      results.put( pos, 1 );

    return results;
    }

  public static ElementSubGraph asSubGraph2( ElementGraph elementGraph, ElementGraph contractedGraph )
    {
    return new ElementSubGraph( elementGraph, findClosureViaBiConnected( elementGraph, contractedGraph ) );
    }

  public static <V, E> Set<V> findClosureViaBiConnected( DirectedGraph<V, E> full, DirectedGraph<V, E> contracted )
    {
    SimpleGraph<V, E> biConnected = (SimpleGraph<V, E>) new SimpleGraph<>( Object.class );

    Graphs.addGraph( biConnected, full );
    Graphs.addGraph( biConnected, contracted );

    BiconnectivityInspector inspector = new BiconnectivityInspector( biConnected );

    LinkedList<Set<V>> components = new LinkedList( inspector.getBiconnectedVertexComponents() );

    if( components.isEmpty() )
      throw new IllegalStateException( "no components" );

    Set<V> vertices = new HashSet<>( contracted.vertexSet() );

    for( E edge : contracted.edgeSet() )
      {
      V edgeSource = contracted.getEdgeSource( edge );
      V edgeTarget = contracted.getEdgeTarget( edge );

      ListIterator<Set<V>> iterator = components.listIterator();
      while( iterator.hasNext() )
        {
        Set<V> set = iterator.next();
        if( set.contains( edgeSource ) && set.contains( edgeTarget ) )
          {
          iterator.remove();
          vertices.addAll( set );
          }
        }
      }

    if( vertices.isEmpty() )
      throw new IllegalStateException( "no vertices" );

    return vertices;
    }

  public static ElementSubGraph asSubGraph( ElementGraph elementGraph, ElementGraph contractedGraph )
    {
    return new ElementSubGraph( elementGraph, findClosureViaFloydWarshall( elementGraph, contractedGraph ) );
    }

  public static <V, E> Set<V> findClosureViaFloydWarshall( DirectedGraph<V, E> full, DirectedGraph<V, E> contracted )
    {
    Set<V> vertices = new HashSet<>( contracted.vertexSet() );
    LinkedList<V> allVertices = new LinkedList<>( full.vertexSet() );

    allVertices.removeAll( vertices );

    Set<E> edges = new HashSet<>();

    for( V v : contracted.vertexSet() )
      {
      if( contracted.inDegreeOf( v ) == 0 )
        edges.addAll( full.incomingEdgesOf( v ) );
      }

    for( V v : contracted.vertexSet() )
      {
      if( contracted.outDegreeOf( v ) == 0 )
        edges.addAll( full.outgoingEdgesOf( v ) );
      }

    DirectedGraph<V, E> disconnected = disconnectExtents( full, edges );

    FloydWarshallShortestPaths<V, E> paths = new FloydWarshallShortestPaths<>( disconnected );

    for( E edge : contracted.edgeSet() )
      {
      V edgeSource = contracted.getEdgeSource( edge );
      V edgeTarget = contracted.getEdgeTarget( edge );

      ListIterator<V> iterator = allVertices.listIterator();
      while( iterator.hasNext() )
        {
        V vertex = iterator.next();

        if( !isBetween( paths, edgeSource, edgeTarget, vertex ) )
          continue;

        vertices.add( vertex );
        iterator.remove();
        }
      }

    return vertices;
    }

  private static <V, E> DirectedGraph<V, E> disconnectExtents( DirectedGraph<V, E> full, Set<E> withoutEdges )
    {
    DirectedGraph<V, E> copy = (DirectedGraph<V, E>) new SimpleDirectedGraph<>( Object.class );

    Graphs.addAllVertices( copy, full.vertexSet() );

    Set<E> edges = full.edgeSet();

    if( !withoutEdges.isEmpty() )
      {
      edges = new HashSet<>( edges );
      edges.removeAll( withoutEdges );
      }

    Graphs.addAllEdges( copy, full, edges );

    return copy;
    }

  private static <V, E> boolean isBetween( FloydWarshallShortestPaths<V, E> paths, V edgeSource, V edgeTarget, V vertex )
    {
    return paths.shortestDistance( edgeSource, vertex ) != POSITIVE_INFINITY && paths.shortestDistance( vertex, edgeTarget ) != POSITIVE_INFINITY;
    }

  public static ElementSubGraph asSubGraphKS( ElementGraph elementGraph, ElementGraph contractedGraph )
    {
    return new ElementSubGraph( elementGraph, findClosureViaKShortest( elementGraph, contractedGraph ) );
    }

  public static <V, E> Set<V> findClosureViaKShortest( DirectedGraph<V, E> elementGraph, DirectedGraph<V, E> contractedGraph )
    {
    // this is bad news, shortest paths is non-linear

    Set<V> allVertices = new HashSet<>( contractedGraph.vertexSet() ); // if no edges, we have the vertices
    Set<E> edges = contractedGraph.edgeSet();

    for( E edge : edges )
      {
      V lhs = contractedGraph.getEdgeSource( edge );
      V rhs = contractedGraph.getEdgeTarget( edge );

      Set<V> otherVertices = new HashSet<>( contractedGraph.vertexSet() );

      otherVertices.remove( lhs );
      otherVertices.remove( rhs );

      List<GraphPath<V, E>> between = getAllShortestPathsBetween( elementGraph, lhs, rhs );

      for( GraphPath<V, E> graphPath : between )
        {
        List<V> pathVertices = Graphs.getPathVertexList( graphPath );

        // do not include a path if it crosses a distinguished element from the contracted graph
        if( Collections.disjoint( pathVertices, otherVertices ) )
          allVertices.addAll( pathVertices );
        }
      }

    return allVertices;
    }

  public static void removeAndContract( ElementGraph elementGraph, FlowElement flowElement )
    {
    LOG.debug( "removing element, contracting edge : " + flowElement );

    Set<Scope> incomingScopes = elementGraph.incomingEdgesOf( flowElement );

    boolean contractIncoming = true;

    if( !contractIncoming )
      {
      if( incomingScopes.size() != 1 )
        throw new IllegalStateException( "flow element:" + flowElement + ", has multiple input paths: " + incomingScopes.size() );
      }

    boolean isJoin = flowElement instanceof Splice && ( (Splice) flowElement ).isJoin();

    for( Scope incoming : incomingScopes )
      {
      Set<Scope> outgoingScopes = elementGraph.outgoingEdgesOf( flowElement );

      // source -> incoming -> flowElement -> outgoing -> target
      FlowElement source = elementGraph.getEdgeSource( incoming );

      for( Scope outgoing : outgoingScopes )
        {
        FlowElement target = elementGraph.getEdgeTarget( outgoing );

        boolean isNonBlocking = outgoing.isNonBlocking();

        if( isJoin )
          isNonBlocking = isNonBlocking && incoming.isNonBlocking();

        Scope scope = new Scope( outgoing );

        // unsure if necessary since we track blocking independently
        // when removing a pipe, pull ordinal up to tap
        // when removing a Splice retain ordinal
        if( flowElement instanceof Splice )
          scope.setOrdinal( incoming.getOrdinal() );
        else
          scope.setOrdinal( outgoing.getOrdinal() );

        scope.setNonBlocking( isNonBlocking );
        scope.addPriorNames( incoming, outgoing ); // not copied
        elementGraph.addEdge( source, target, scope );
        }
      }

    elementGraph.removeVertex( flowElement );
    }

  public static void printElementGraph( String filename, final DirectedGraph<FlowElement, Scope> graph, final PlatformInfo platformInfo )
    {
    try
      {
      File parentFile = new File( filename ).getParentFile();

      if( parentFile != null && !parentFile.exists() )
        parentFile.mkdirs();

      Writer writer = new FileWriter( filename );

      Util.writeDOT( writer, graph,
        new IntegerNameProvider<FlowElement>(),
        new FlowElementVertexNameProvider( graph, platformInfo ),
        new ScopeEdgeNameProvider(),
        new VertexAttributeProvider( graph ), new EdgeAttributeProvider() );

      writer.close();
      }
    catch( IOException exception )
      {
      LOG.error( "failed printing graph to: {}, with exception: {}", filename, exception );
      }
    }

  public static void insertFlowElementAfter( ElementGraph elementGraph, FlowElement previousElement, FlowElement flowElement )
    {
    Set<Scope> outgoing = new HashSet<Scope>( elementGraph.outgoingEdgesOf( previousElement ) );

    elementGraph.addVertex( flowElement );

    String name = previousElement.toString();

    if( previousElement instanceof Pipe )
      name = ( (Pipe) previousElement ).getName();

    elementGraph.addEdge( previousElement, flowElement, new Scope( name ) );

    for( Scope scope : outgoing )
      {
      FlowElement target = elementGraph.getEdgeTarget( scope );
      Scope foundScope = elementGraph.removeEdge( previousElement, target );// remove scope

      if( foundScope != scope )
        throw new IllegalStateException( "did not remove proper scope" );

      elementGraph.addEdge( flowElement, target, scope ); // add scope back
      }
    }

  public static void addSources( BaseFlowStep flowStep, ElementGraph elementGraph, Set<Tap> sources )
    {
    for( Tap tap : sources )
      {
      for( Scope scope : elementGraph.outgoingEdgesOf( tap ) )
        flowStep.addSource( scope.getName(), tap );
      }
    }

  public static Set<Tap> findSources( ElementGraph elementGraph )
    {
    return findSources( elementGraph, Tap.class );
    }

  public static <F extends FlowElement> Set<F> findSources( ElementGraph elementGraph, Class<F> type )
    {
    if( elementGraph.containsVertex( Extent.head ) )
      elementGraph = new ElementMaskSubGraph( elementGraph, Extent.head, Extent.tail ); // TODO: just return the successors

    SubGraphIterator iterator = new SubGraphIterator(
      new ExpressionGraph( SearchOrder.Topological, new FlowElementExpression( ElementExpression.Capture.Primary, type, TypeExpression.Topo.Head ) ),
      elementGraph
    );

    return narrowSet( type, getAllVertices( iterator ) );
    }

  public static <F extends FlowElement> Set<F> findSinks( ElementGraph elementGraph, Class<F> type )
    {
    if( elementGraph.containsVertex( Extent.tail ) )
      elementGraph = new ElementMaskSubGraph( elementGraph, Extent.head, Extent.tail ); // TODO: just return the predecessors

    SubGraphIterator iterator = new SubGraphIterator(
      new ExpressionGraph( SearchOrder.ReverseTopological, new FlowElementExpression( ElementExpression.Capture.Primary, type, TypeExpression.Topo.Tail ) ),
      elementGraph
    );

    return narrowSet( type, getAllVertices( iterator ) );
    }

  public static void addSinks( BaseFlowStep flowStep, ElementGraph elementGraph, Set<Tap> sinks )
    {
    for( Tap tap : sinks )
      {
      for( Scope scope : elementGraph.incomingEdgesOf( tap ) )
        flowStep.addSink( scope.getName(), tap );
      }
    }

  public static Set<Tap> findSinks( ElementGraph elementGraph )
    {
    return findSinks( elementGraph, Tap.class );
    }

  public static Set<Group> findAllGroups( ElementGraph elementGraph )
    {
    SubGraphIterator iterator = new SubGraphIterator(
      new ExpressionGraph( SearchOrder.Topological, new FlowElementExpression( ElementExpression.Capture.Primary, Group.class ) ),
      elementGraph
    );

    return narrowSet( Group.class, getAllVertices( iterator ) );
    }

  public static Set<HashJoin> findAllHashJoins( ElementGraph elementGraph )
    {
    SubGraphIterator iterator = new SubGraphIterator(
      new ExpressionGraph( SearchOrder.Topological, new FlowElementExpression( ElementExpression.Capture.Primary, HashJoin.class ) ),
      elementGraph
    );

    return narrowSet( HashJoin.class, getAllVertices( iterator ) );
    }

  private static Set<FlowElement> getAllVertices( SubGraphIterator iterator )
    {
    Set<FlowElement> vertices = new HashSet<>();

    while( iterator.hasNext() )
      vertices.addAll( iterator.next().vertexSet() );

    return vertices;
    }

  public static void replaceElementWith( ElementGraph elementGraph, FlowElement replace, FlowElement replaceWith )
    {
    Set<Scope> incoming = new HashSet<Scope>( elementGraph.incomingEdgesOf( replace ) );
    Set<Scope> outgoing = new HashSet<Scope>( elementGraph.outgoingEdgesOf( replace ) );

    if( !elementGraph.containsVertex( replaceWith ) )
      elementGraph.addVertex( replaceWith );

    for( Scope scope : incoming )
      {
      FlowElement source = elementGraph.getEdgeSource( scope );
      elementGraph.removeEdge( source, replace ); // remove scope

      // drop edge between, if any
      if( source != replaceWith )
        elementGraph.addEdge( source, replaceWith, scope ); // add scope back
      }

    for( Scope scope : outgoing )
      {
      FlowElement target = elementGraph.getEdgeTarget( scope );
      elementGraph.removeEdge( replace, target ); // remove scope

      // drop edge between, if any
      if( target != replaceWith )
        elementGraph.addEdge( replaceWith, target, scope ); // add scope back
      }

    elementGraph.removeVertex( replace );
    }

  public static Pipe findFirstPipeNamed( ElementGraph elementGraph, String name )
    {
    Iterator<FlowElement> iterator = getTopologicalIterator( elementGraph );

    while( iterator.hasNext() )
      {
      FlowElement flowElement = iterator.next();

      if( flowElement instanceof Pipe && ( (Pipe) flowElement ).getName().equals( name ) )
        return (Pipe) flowElement;
      }

    return null;
    }

  public static void removeBranchContaining( ElementGraph elementGraph, FlowElement flowElement )
    {
    Set<FlowElement> branch = new LinkedHashSet<>();

    // if both a join and split, cannot be removed
    FlowElement current = flowElement;

    while( true )
      {
      if( elementGraph.inDegreeOf( current ) != 1 || elementGraph.outDegreeOf( current ) != 1 )
        break;

      branch.add( current );

      FlowElement element = elementGraph.getEdgeSource( getFirst( elementGraph.incomingEdgesOf( current ) ) );

      if( element instanceof Extent )
        break;

      current = element;
      }

    current = flowElement;

    while( true )
      {
      if( elementGraph.inDegreeOf( current ) != 1 || elementGraph.outDegreeOf( current ) != 1 )
        break;

      branch.add( current );

      FlowElement element = elementGraph.getEdgeTarget( getFirst( elementGraph.outgoingEdgesOf( current ) ) );

      if( element instanceof Extent )
        break;

      current = element;
      }

    for( FlowElement element : branch )
      elementGraph.removeVertex( element );
    }

  private static class FlowElementVertexNameProvider implements VertexNameProvider<FlowElement>
    {
    private final DirectedGraph<FlowElement, Scope> graph;
    private final PlatformInfo platformInfo;

    public FlowElementVertexNameProvider( DirectedGraph<FlowElement, Scope> graph, PlatformInfo platformInfo )
      {
      this.graph = graph;
      this.platformInfo = platformInfo;
      }

    public String getVertexName( FlowElement object )
      {
      if( graph.incomingEdgesOf( object ).isEmpty() ) // is head
        {
        String result = object.toString().replaceAll( "\"", "\'" );
        String versionString = Version.getRelease();

        if( platformInfo != null )
          versionString = ( versionString == null ? "" : versionString + "\\n" ) + platformInfo;

        return versionString == null ? result : result + "\\n" + versionString;
        }

      Iterator<Scope> iterator = graph.outgoingEdgesOf( object ).iterator();

      if( object instanceof Tap || object instanceof Extent || !iterator.hasNext() )
        return object.toString().replaceAll( "\"", "\'" ).replaceAll( "(\\)|\\])(\\[)", "$1\\\\n$2" ).replaceAll( "(^[^(\\[]+)(\\(|\\[)", "$1\\\\n$2" );

      Scope scope = iterator.next();

      return ( (Pipe) object ).print( scope ).replaceAll( "\"", "\'" ).replaceAll( "(\\)|\\])(\\[)", "$1\\\\n$2" ).replaceAll( "(^[^(\\[]+)(\\(|\\[)", "$1\\\\n$2" );
      }
    }

  private static class ScopeEdgeNameProvider implements EdgeNameProvider<Scope>
    {
    public String getEdgeName( Scope object )
      {
      return object.toString().replaceAll( "\"", "\'" ).replaceAll( "\n", "\\\\n" ); // fix for newlines in graphviz
      }
    }

  private static class VertexAttributeProvider implements ComponentAttributeProvider<FlowElement>
    {
    static Map<String, String> streamedNode = new HashMap<String, String>()
    {
    {put( "shape", "box" );}

    {put( "fontcolor", "blue1" );}
    };

    static Map<String, String> defaultNode = new HashMap<String, String>()
    {
    {put( "shape", "box" );}
    };

    private final AnnotatedElementGraph graph;

    public VertexAttributeProvider( DirectedGraph<FlowElement, Scope> graph )
      {
      if( graph instanceof AnnotatedElementGraph )
        this.graph = (AnnotatedElementGraph) graph;
      else
        this.graph = null;
      }

    @Override
    public Map<String, String> getComponentAttributes( FlowElement object )
      {
      if( graph == null || !( object instanceof Tap ) )
        return defaultNode;

      Set<FlowElement> elements = graph.getAnnotations().get( StreamMode.Streamed );

      boolean hasAnnotations = elements != null && !elements.isEmpty();

      if( !hasAnnotations )
        return defaultNode;

      boolean isStreamed = hasAnnotations && elements.contains( object );

      return isStreamed ? streamedNode : defaultNode;
      }
    }

  private static class EdgeAttributeProvider implements ComponentAttributeProvider<Scope>
    {
    static Map<String, String> attributes = new HashMap<String, String>()
    {
    {put( "style", "dotted" );}
    };

    @Override
    public Map<String, String> getComponentAttributes( Scope scope )
      {
      if( scope.isNonBlocking() )
        return null;

      return attributes;
      }
    }
  }
