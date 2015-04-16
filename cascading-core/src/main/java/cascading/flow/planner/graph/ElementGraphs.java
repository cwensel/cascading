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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowElements;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.Scope;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.flow.planner.iso.subgraph.SubGraphIterator;
import cascading.flow.planner.iso.subgraph.iterator.ExpressionSubGraphIterator;
import cascading.flow.planner.process.ProcessGraph;
import cascading.flow.planner.process.ProcessModel;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.pipe.Operator;
import cascading.pipe.Pipe;
import cascading.pipe.Splice;
import cascading.tap.Tap;
import cascading.util.DOTProcessGraphWriter;
import cascading.util.Murmur3;
import cascading.util.Pair;
import cascading.util.Util;
import cascading.util.Version;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.alg.FloydWarshallShortestPaths;
import org.jgrapht.alg.KShortestPaths;
import org.jgrapht.ext.ComponentAttributeProvider;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.AbstractGraph;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.jgrapht.util.TypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.Util.getFirst;
import static cascading.util.Util.narrowSet;
import static java.lang.Double.POSITIVE_INFINITY;

/**
 * Class ElementGraphs maintains a collection of operations that can be performed on an {@link ElementGraph}.
 */
public class ElementGraphs
  {
  private static final Logger LOG = LoggerFactory.getLogger( ElementGraphs.class );

  // not for instantiation
  private ElementGraphs()
    {
    }

  public static DirectedGraph<FlowElement, Scope> directed( ElementGraph elementGraph )
    {
    if( elementGraph instanceof DecoratedElementGraph )
      return directed( ( (DecoratedElementGraph) elementGraph ).getDecorated() );

    return ( (BaseElementGraph) elementGraph ).graph;
    }

  public static int hashCodeIgnoreAnnotations( ElementGraph elementGraph )
    {
    return hashCodeIgnoreAnnotations( directed( elementGraph ) );
    }

  public static <V, E> int hashCodeIgnoreAnnotations( Graph<V, E> graph )
    {
    int hash = graph.vertexSet().hashCode();

    for( E e : graph.edgeSet() )
      {
      int part = e.hashCode();

      int source = graph.getEdgeSource( e ).hashCode();
      int target = graph.getEdgeTarget( e ).hashCode();

      int pairing = pair( source, target );

      part = ( 27 * part ) + pairing;

      long weight = (long) graph.getEdgeWeight( e );
      part = ( 27 * part ) + (int) ( weight ^ ( weight >>> 32 ) );

      hash += part;
      }

    return hash;
    }

  public static boolean equalsIgnoreAnnotations( ElementGraph lhs, ElementGraph rhs )
    {
    return equalsIgnoreAnnotations( directed( lhs ), directed( rhs ) );
    }

  public static <V, E> boolean equalsIgnoreAnnotations( Graph<V, E> lhs, Graph<V, E> rhs )
    {
    if( lhs == rhs )
      return true;

    TypeUtil<Graph<V, E>> typeDecl = null;
    Graph<V, E> lhsGraph = TypeUtil.uncheckedCast( lhs, typeDecl );
    Graph<V, E> rhsGraph = TypeUtil.uncheckedCast( rhs, typeDecl );

    if( !lhsGraph.vertexSet().equals( rhsGraph.vertexSet() ) )
      return false;

    if( lhsGraph.edgeSet().size() != rhsGraph.edgeSet().size() )
      return false;

    for( E e : lhsGraph.edgeSet() )
      {
      V source = lhsGraph.getEdgeSource( e );
      V target = lhsGraph.getEdgeTarget( e );

      if( !rhsGraph.containsEdge( e ) )
        return false;

      if( !rhsGraph.getEdgeSource( e ).equals( source ) || !rhsGraph.getEdgeTarget( e ).equals( target ) )
        return false;

      if( Math.abs( lhsGraph.getEdgeWeight( e ) - rhsGraph.getEdgeWeight( e ) ) > 10e-7 )
        return false;
      }

    return true;
    }

  public static boolean equals( ElementGraph lhs, ElementGraph rhs )
    {
    if( !equalsIgnoreAnnotations( lhs, rhs ) )
      return false;

    if( !( lhs instanceof AnnotatedGraph ) && !( rhs instanceof AnnotatedGraph ) )
      return true;

    if( !( lhs instanceof AnnotatedGraph ) || !( rhs instanceof AnnotatedGraph ) )
      return false;

    AnnotatedGraph lhsAnnotated = (AnnotatedGraph) lhs;
    AnnotatedGraph rhsAnnotated = (AnnotatedGraph) rhs;

    if( lhsAnnotated.hasAnnotations() != rhsAnnotated.hasAnnotations() )
      return false;

    if( !lhsAnnotated.hasAnnotations() )
      return true;

    return lhsAnnotated.getAnnotations().equals( rhsAnnotated.getAnnotations() );
    }

  public static String canonicalHash( ElementGraph graph )
    {
    return canonicalHash( directed( graph ) );
    }

  public static String canonicalHash( Graph<FlowElement, Scope> graph )
    {
    int hash = Murmur3.SEED;

    int edges = 0;
    boolean hasExtents = false;

    for( Scope e : graph.edgeSet() )
      {
      FlowElement edgeSource = graph.getEdgeSource( e );
      FlowElement edgeTarget = graph.getEdgeTarget( e );

      // simpler to ignore extents here
      if( edgeSource instanceof Extent || edgeTarget instanceof Extent )
        {
        hasExtents = true;
        continue;
        }

      int source = hash( edgeSource );
      int target = hash( edgeTarget );
      int pairing = pair( source, target );

      hash += pairing; // don't make edge traversal order significant
      edges++;
      }

    int vertexes = graph.vertexSet().size() - ( hasExtents ? 2 : 0 );

    hash = Murmur3.fmix( hash, vertexes * edges );

    return Util.getHex( Util.intToByteArray( hash ) );
    }

  private static int hash( FlowElement flowElement )
    {
    int lhs = flowElement.getClass().getName().hashCode();
    int rhs = 0;

    if( flowElement instanceof Operator )
      rhs = ( (Operator) flowElement ).getOperation().getClass().getName().hashCode();
    else if( flowElement instanceof Tap )
      rhs = ( (Tap) flowElement ).getIdentifier().hashCode();
    else if( flowElement instanceof Splice )
      rhs = ( (Splice) flowElement ).getJoiner().getClass().getName().hashCode() + 31 * ( (Splice) flowElement ).getNumSelfJoins();

    return pair( lhs, rhs );
    }

  protected static int pair( int lhs, int rhs )
    {
    if( rhs == 0 )
      return lhs;

    // see http://en.wikipedia.org/wiki/Pairing_function
    return ( ( lhs + rhs ) * ( lhs + rhs + 1 ) / 2 ) + rhs;
    }

  public static TopologicalOrderIterator<FlowElement, Scope> getTopologicalIterator( ElementGraph graph )
    {
    return new TopologicalOrderIterator<>( directed( graph ) );
    }

  public static TopologicalOrderIterator<FlowElement, Scope> getReverseTopologicalIterator( ElementGraph graph )
    {
    return new TopologicalOrderIterator<>( new EdgeReversedGraph<>( directed( graph ) ) );
    }

  public static List<GraphPath<FlowElement, Scope>> getAllShortestPathsBetween( ElementGraph graph, FlowElement from, FlowElement to )
    {
    return getAllShortestPathsBetween( directed( graph ), from, to );
    }

  public static <V, E> List<GraphPath<V, E>> getAllShortestPathsBetween( DirectedGraph<V, E> graph, V from, V to )
    {
    List<GraphPath<V, E>> paths = new KShortestPaths<>( graph, from, Integer.MAX_VALUE ).getPaths( to );

    if( paths == null )
      return new ArrayList<>();

    return paths;
    }

  public static ElementSubGraph asSubGraph( ElementGraph elementGraph, ElementGraph contractedGraph, Set<FlowElement> excludes )
    {
    elementGraph = asExtentMaskedSubGraph( elementGraph ); // returns same instance if not bounded

    Pair<Set<FlowElement>, Set<Scope>> pair = findClosureViaFloydWarshall( directed( elementGraph ), directed( contractedGraph ), excludes );
    Set<FlowElement> vertices = pair.getLhs();
    Set<Scope> excludeEdges = pair.getRhs();

    Set<Scope> scopes = new HashSet<>( elementGraph.edgeSet() );
    scopes.removeAll( excludeEdges );

    return new ElementSubGraph( elementGraph, vertices, scopes );
    }

  /**
   * Returns a new ElementGraph (a MaskedSubGraph) of the given ElementGraph that will not contain the {@link Extent}
   * head or tail instances.
   * <p/>
   * If the given ElementGraph does not contain head or tail, it will be returned unchanged.
   *
   * @param elementGraph
   * @return
   */
  public static ElementGraph asExtentMaskedSubGraph( ElementGraph elementGraph )
    {
    if( elementGraph.containsVertex( Extent.head ) || elementGraph.containsVertex( Extent.tail ) )
      return new ElementMaskSubGraph( elementGraph, Extent.head, Extent.tail );

    return elementGraph;
    }

  public static <V, E> Pair<Set<V>, Set<E>> findClosureViaFloydWarshall( DirectedGraph<V, E> full, DirectedGraph<V, E> contracted )
    {
    return findClosureViaFloydWarshall( full, contracted, null );
    }

  public static <V, E> Pair<Set<V>, Set<E>> findClosureViaFloydWarshall( DirectedGraph<V, E> full, DirectedGraph<V, E> contracted, Set<V> excludes )
    {
    Set<V> vertices = new HashSet<>( contracted.vertexSet() );
    LinkedList<V> allVertices = new LinkedList<>( full.vertexSet() );

    allVertices.removeAll( vertices );

    Set<E> excludeEdges = new HashSet<>();

    // prevent distinguished elements from being included inside the sub-graph
    if( excludes != null )
      {
      for( V v : excludes )
        {
        if( !full.containsVertex( v ) )
          continue;

        excludeEdges.addAll( full.incomingEdgesOf( v ) );
        excludeEdges.addAll( full.outgoingEdgesOf( v ) );
        }
      }

    for( V v : contracted.vertexSet() )
      {
      if( contracted.inDegreeOf( v ) == 0 )
        excludeEdges.addAll( full.incomingEdgesOf( v ) );
      }

    for( V v : contracted.vertexSet() )
      {
      if( contracted.outDegreeOf( v ) == 0 )
        excludeEdges.addAll( full.outgoingEdgesOf( v ) );
      }

    DirectedGraph<V, E> disconnected = disconnectExtentsAndExclude( full, excludeEdges );

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

    return new Pair<>( vertices, excludeEdges );
    }

  private static <V, E> DirectedGraph<V, E> disconnectExtentsAndExclude( DirectedGraph<V, E> full, Set<E> withoutEdges )
    {
    DirectedGraph<V, E> copy = (DirectedGraph<V, E>) new SimpleDirectedGraph<>( Object.class );

    Graphs.addAllVertices( copy, full.vertexSet() );

    copy.removeVertex( (V) Extent.head );
    copy.removeVertex( (V) Extent.tail );

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

  public static void removeAndContract( ElementGraph elementGraph, FlowElement flowElement )
    {
    LOG.debug( "removing element, contracting edge for: {}", flowElement );

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

  public static boolean printElementGraph( String filename, final DirectedGraph<FlowElement, Scope> graph, final PlatformInfo platformInfo )
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
        new VertexAttributeProvider(), new EdgeAttributeProvider() );

      writer.close();
      return true;
      }
    catch( IOException exception )
      {
      LOG.error( "failed printing graph to: {}, with exception: {}", filename, exception );
      }

    return false;
    }

  public static boolean printProcessGraph( String filename, final ElementGraph graph, final ProcessGraph<? extends ProcessModel> processGraph )
    {
    try
      {
      File parentFile = new File( filename ).getParentFile();

      if( parentFile != null && !parentFile.exists() )
        parentFile.mkdirs();

      Writer writer = new FileWriter( filename );

      DOTProcessGraphWriter graphWriter = new DOTProcessGraphWriter(
        new IntegerNameProvider<Pair<ElementGraph, FlowElement>>(),
        new FlowElementVertexNameProvider( directed( graph ), null ),
        new ScopeEdgeNameProvider(),
        new VertexAttributeProvider(), new EdgeAttributeProvider(),
        new ProcessGraphNameProvider(), new ProcessGraphLabelProvider()
      );

      graphWriter.writeGraph( writer, graph, processGraph );

      writer.close();
      return true;
      }
    catch( IOException exception )
      {
      LOG.error( "failed printing graph to: {}, with exception: {}", filename, exception );
      }

    return false;
    }

  public static void insertFlowElementAfter( ElementGraph elementGraph, FlowElement previousElement, FlowElement flowElement )
    {
    Set<Scope> outgoing = new HashSet<>( elementGraph.outgoingEdgesOf( previousElement ) );

    elementGraph.addVertex( flowElement );

    String name = previousElement.toString();

    if( previousElement instanceof Pipe )
      name = ( (Pipe) previousElement ).getName();

    elementGraph.addEdge( previousElement, flowElement, new Scope( name ) );

    for( Scope scope : outgoing )
      {
      FlowElement target = elementGraph.getEdgeTarget( scope );
      Scope foundScope = elementGraph.removeEdge( previousElement, target ); // remove scope

      if( foundScope != scope )
        throw new IllegalStateException( "did not remove proper scope" );

      elementGraph.addEdge( flowElement, target, scope ); // add scope back
      }
    }

  public static void insertFlowElementBefore( ElementGraph graph, FlowElement nextElement, FlowElement flowElement )
    {
    Set<Scope> incoming = new HashSet<>( graph.incomingEdgesOf( nextElement ) );

    graph.addVertex( flowElement );

    String name = nextElement.toString();

    if( nextElement instanceof Pipe )
      name = ( (Pipe) nextElement ).getName();

    graph.addEdge( flowElement, nextElement, new Scope( name ) );

    for( Scope scope : incoming )
      {
      FlowElement target = graph.getEdgeSource( scope );
      Scope foundScope = graph.removeEdge( target, nextElement ); // remove scope

      if( foundScope != scope )
        throw new IllegalStateException( "did not remove proper scope" );

      graph.addEdge( target, flowElement, scope ); // add scope back
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
      return narrowSet( type, elementGraph.successorListOf( Extent.head ) );

    SubGraphIterator iterator = new ExpressionSubGraphIterator(
      new ExpressionGraph( SearchOrder.Topological, new FlowElementExpression( ElementCapture.Primary, type, TypeExpression.Topo.Head ) ),
      elementGraph
    );

    return narrowSet( type, getAllVertices( iterator ) );
    }

  public static <F extends FlowElement> Set<F> findSinks( ElementGraph elementGraph, Class<F> type )
    {
    if( elementGraph.containsVertex( Extent.tail ) )
      return narrowSet( type, elementGraph.predecessorListOf( Extent.tail ) );

    SubGraphIterator iterator = new ExpressionSubGraphIterator(
      new ExpressionGraph( SearchOrder.ReverseTopological, new FlowElementExpression( ElementCapture.Primary, type, TypeExpression.Topo.Tail ) ),
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
    SubGraphIterator iterator = new ExpressionSubGraphIterator(
      new ExpressionGraph( SearchOrder.Topological, new FlowElementExpression( ElementCapture.Primary, Group.class ) ),
      elementGraph
    );

    return narrowSet( Group.class, getAllVertices( iterator ) );
    }

  public static Set<HashJoin> findAllHashJoins( ElementGraph elementGraph )
    {
    SubGraphIterator iterator = new ExpressionSubGraphIterator(
      new ExpressionGraph( SearchOrder.Topological, new FlowElementExpression( ElementCapture.Primary, HashJoin.class ) ),
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

    return find( name, iterator );
    }

  public static Pipe findLastPipeNamed( ElementGraph elementGraph, String name )
    {
    Iterator<FlowElement> iterator = getReverseTopologicalIterator( elementGraph );

    return find( name, iterator );
    }

  private static Pipe find( String name, Iterator<FlowElement> iterator )
    {
    while( iterator.hasNext() )
      {
      FlowElement flowElement = iterator.next();

      if( flowElement instanceof Pipe && ( (Pipe) flowElement ).getName().equals( name ) )
        return (Pipe) flowElement;
      }

    return null;
    }

  public static boolean removeBranchContaining( ElementGraph elementGraph, FlowElement flowElement )
    {
    Set<FlowElement> branch = new LinkedHashSet<>();

    walkUp( branch, elementGraph, flowElement );

    walkDown( branch, elementGraph, flowElement );

    if( branch.isEmpty() )
      return false;

    for( FlowElement element : branch )
      elementGraph.removeVertex( element );

    return true;
    }

  public static boolean removeBranchBetween( ElementGraph elementGraph, FlowElement first, FlowElement second, boolean inclusive )
    {
    Set<FlowElement> branch = new LinkedHashSet<>( Arrays.asList( first, second ) );

    walkDown( branch, elementGraph, first );

    if( !inclusive )
      {
      branch.remove( first );
      branch.remove( second );
      }

    if( branch.isEmpty() )
      return false;

    for( FlowElement element : branch )
      elementGraph.removeVertex( element );

    return true;
    }

  private static void walkDown( Set<FlowElement> branch, ElementGraph elementGraph, FlowElement flowElement )
    {
    FlowElement current;
    current = flowElement;

    while( true )
      {
      if( !branch.contains( current ) && ( elementGraph.inDegreeOf( current ) != 1 || elementGraph.outDegreeOf( current ) != 1 ) )
        break;

      branch.add( current );

      FlowElement element = elementGraph.getEdgeTarget( getFirst( elementGraph.outgoingEdgesOf( current ) ) );

      if( element instanceof Extent || branch.contains( element ) )
        break;

      current = element;
      }
    }

  private static void walkUp( Set<FlowElement> branch, ElementGraph elementGraph, FlowElement flowElement )
    {
    FlowElement current = flowElement;

    while( true )
      {
      if( elementGraph.inDegreeOf( current ) != 1 || elementGraph.outDegreeOf( current ) != 1 )
        break;

      branch.add( current );

      FlowElement element = elementGraph.getEdgeSource( getFirst( elementGraph.incomingEdgesOf( current ) ) );

      if( element instanceof Extent || branch.contains( element ) )
        break;

      current = element;
      }
    }

  /**
   * Returns the number of edges found on the shortest distance between the lhs and rhs.
   */
  public static int shortestDistance( ElementGraph graph, FlowElement lhs, FlowElement rhs )
    {
    return DijkstraShortestPath.findPathBetween( directed( graph ), lhs, rhs ).size();
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
      if( object instanceof Extent ) // is head/tail
        {
        String result = object.toString().replaceAll( "\"", "\'" );

        if( object == Extent.tail )
          return result;

        result = result + "|hash: " + canonicalHash( graph );

        String versionString = Version.getRelease();

        if( platformInfo != null )
          versionString = ( versionString == null ? "" : versionString + "|" ) + platformInfo;

        return "{" + ( versionString == null ? result : result + "|" + versionString ) + "}";
        }

      String label;

      Iterator<Scope> iterator = graph.outgoingEdgesOf( object ).iterator();

      if( object instanceof Tap || !iterator.hasNext() )
        {
        label = object.toString().replaceAll( "\"", "\'" ).replaceAll( "(\\)|\\])(\\[)", "$1|$2" ).replaceAll( "(^[^(\\[]+)(\\(|\\[)", "$1|$2" );
        }
      else
        {
        Scope scope = iterator.next();

        label = ( (Pipe) object ).print( scope ).replaceAll( "\"", "\'" ).replaceAll( "(\\)|\\])(\\[)", "$1|$2" ).replaceAll( "(^[^(\\[]+)(\\(|\\[)", "$1|$2" );
        }

      label = label.replaceFirst( "([^|]+)\\|(.*)", "$1 : " + getID( object ) + "|$2" ); // insert id

      label = "{" + label.replaceAll( "\\{", "\\\\{" ).replaceAll( "\\}", "\\\\}" ).replaceAll( ">", "\\\\>" ) + "}";

      if( !( graph instanceof AnnotatedGraph ) || !( (AnnotatedGraph) graph ).hasAnnotations() )
        return label;

      Set<Enum> annotations = ( (AnnotatedGraph) graph ).getAnnotations().getKeysFor( object );

      if( !annotations.isEmpty() )
        label += "|{" + Util.join( annotations, "|" ) + "}";

      return label;
      }

    protected String getID( FlowElement object )
      {
      return FlowElements.id( object ).substring( 0, 5 );
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
    static Map<String, String> defaultNode = new HashMap<String, String>()
    {
    {put( "shape", "Mrecord" );}
    };

    public VertexAttributeProvider()
      {
      }

    @Override
    public Map<String, String> getComponentAttributes( FlowElement object )
      {
      return defaultNode;
      }
    }

  private static class EdgeAttributeProvider implements ComponentAttributeProvider<Scope>
    {
    static Map<String, String> attributes = new HashMap<String, String>()
    {
    {put( "style", "dotted" );}

    {put( "arrowhead", "dot" );}
    };

    @Override
    public Map<String, String> getComponentAttributes( Scope scope )
      {
      if( scope.isNonBlocking() )
        return null;

      return attributes;
      }
    }

  private static class ProcessGraphNameProvider implements VertexNameProvider<ProcessModel>
    {
    @Override
    public String getVertexName( ProcessModel processModel )
      {
      return "" + processModel.getOrdinal();
      }
    }

  private static class ProcessGraphLabelProvider implements VertexNameProvider<ProcessModel>
    {
    @Override
    public String getVertexName( ProcessModel processModel )
      {
      return "ordinal: " + processModel.getOrdinal() +
        "\\nid: " + processModel.getID() +
        "\\nhash: " + canonicalHash( processModel.getElementGraph() );
      }
    }

  static void injectIdentityMap( AbstractGraph graph )
    {
    // this overcomes jgrapht 0.9.0 using a LinkedHashMap vs an IdentityHashMap
    // vertex not found errors will be thrown if this fails
    Object specifics = Util.returnInstanceFieldIfExistsSafe( graph, "specifics" );

    if( specifics == null )
      {
      LOG.warn( "unable to get jgrapht Specifics for identity map injection, may be using an incompatible jgrapht version" );
      return;
      }

    boolean success = Util.setInstanceFieldIfExistsSafe( specifics, "vertexMapDirected", new IdentityHashMap<>() );

    if( !success )
      LOG.warn( "unable to set IdentityHashMap on jgrapht Specifics, may be using an incompatible jgrapht version" );
    }
  }
