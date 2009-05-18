/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow;

import cascading.operation.AssertionLevel;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.Operator;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
import cascading.util.Util;
import org.apache.log4j.Logger;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.KShortestPaths;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.DepthFirstIterator;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Class ElementGraph represents the executable FlowElement graph. */
public class ElementGraph extends SimpleDirectedGraph<FlowElement, Scope>
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( ElementGraph.class );

  /** Field head */
  public static final Extent head = new Extent( "head" );
  /** Field tail */
  public static final Extent tail = new Extent( "tail" );
  /** Field resolved */
  private boolean resolved;
  /** Field sources */
  private Map<String, Tap> sources;
  /** Field sinks */
  private Map<String, Tap> sinks;
  /** Field traps */
  private Map<String, Tap> traps;
  /** Field assertionLevel */
  private AssertionLevel assertionLevel;


  ElementGraph()
    {
    super( Scope.class );
    }

  /**
   * Constructor ElementGraph creates a new ElementGraph instance.
   *
   * @param pipes          of type Pipe[]
   * @param sources        of type Map<String, Tap>
   * @param sinks          of type Map<String, Tap>
   * @param assertionLevel of type AssertionLevel
   */
  public ElementGraph( Pipe[] pipes, Map<String, Tap> sources, Map<String, Tap> sinks, Map<String, Tap> traps, AssertionLevel assertionLevel )
    {
    super( Scope.class );
    this.sources = sources;
    this.sinks = sinks;
    this.traps = traps;
    this.assertionLevel = assertionLevel;

    assembleGraph( pipes, sources, sinks );

    verifyGraph();
    }

  public Collection<Tap> getSources()
    {
    return sources.values();
    }

  public Collection<Tap> getSinks()
    {
    return sinks.values();
    }

  public Collection<Tap> getTraps()
    {
    return traps.values();
    }

  private void assembleGraph( Pipe[] pipes, Map<String, Tap> sources, Map<String, Tap> sinks )
    {
    HashMap<String, Tap> sourcesCopy = new HashMap<String, Tap>( sources );
    HashMap<String, Tap> sinksCopy = new HashMap<String, Tap>( sinks );

    for( Pipe pipe : pipes )
      makeGraph( pipe, sourcesCopy, sinksCopy );

    addExtents( sources, sinks );
    }

  /** Method verifyGraphConnections ... */
  private void verifyGraph()
    {
    if( vertexSet().isEmpty() )
      return;

    // need to verify that only Extent instances are origins in this graph. Otherwise a Tap was not properly connected
    TopologicalOrderIterator<FlowElement, Scope> iterator = getTopologicalIterator();

    while( iterator.hasNext() )
      {
      FlowElement flowElement = iterator.next();

      if( incomingEdgesOf( flowElement ).size() != 0 )
        break;

      if( flowElement instanceof Extent )
        continue;

      if( flowElement instanceof Pipe )
        throw new ElementGraphException( flowElement, "no Tap instance given to connect Pipe " + flowElement.toString() );
      else if( flowElement instanceof Tap )
        throw new ElementGraphException( flowElement, "no Pipe instance given to connect Tap " + flowElement.toString() );
      else
        throw new ElementGraphException( flowElement, "unknown element type: " + flowElement );
      }
    }

  /**
   * Method copyGraph returns a partial copy of the current ElementGraph. Only Vertices and Edges are copied.
   *
   * @return ElementGraph
   */
  public ElementGraph copyElementGraph()
    {
    ElementGraph copy = new ElementGraph();
    Graphs.addGraph( copy, this );

    copy.traps = new HashMap<String, Tap>( this.traps );

    return copy;
    }

  /**
   * created to support the ability to generate all paths between the head and tail of the process.
   *
   * @param sources
   * @param sinks
   */
  private void addExtents( Map<String, Tap> sources, Map<String, Tap> sinks )
    {
    addVertex( head );

    for( String source : sources.keySet() )
      {
      Scope scope = addEdge( head, sources.get( source ) );

      // edge may already exist, if so, above returns null
      if( scope != null )
        scope.setName( source );
      }

    addVertex( tail );

    for( String sink : sinks.keySet() )
      {
      Scope scope = addEdge( sinks.get( sink ), tail );

      if( scope == null )
        throw new ElementGraphException( "cannot sink to the same path from multiple branches: [" + Util.join( sinks.values() ) + "]" );

      scope.setName( sink );
      }
    }

  /**
   * Perfoms one rule check, verifies group does not join duplicate tap resources.
   * <p/>
   * Scopes are always named after the source side of the source -> target relationship
   */
  private void makeGraph( Pipe current, Map<String, Tap> sources, Map<String, Tap> sinks )
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "adding pipe: " + current );

    if( current instanceof SubAssembly )
      {
      for( Pipe pipe : SubAssembly.unwind( current.getPrevious() ) )
        makeGraph( pipe, sources, sinks );

      return;
      }

    if( containsVertex( current ) )
      return;

    addVertex( current );

    Tap sink = sinks.remove( current.getName() );

    if( sink != null )
      {
      if( LOG.isDebugEnabled() )
        LOG.debug( "adding sink: " + sink );

      addVertex( sink );

      if( LOG.isDebugEnabled() )
        LOG.debug( "adding edge: " + current + " -> " + sink );

      addEdge( current, sink ).setName( current.getName() ); // name scope after sink
      }

    // PipeAssemblies should always have a previous
    if( SubAssembly.unwind( current.getPrevious() ).length == 0 )
      {
      Tap source = sources.remove( current.getName() );

      if( source != null )
        {
        if( LOG.isDebugEnabled() )
          LOG.debug( "adding source: " + source );

        addVertex( source );

        if( LOG.isDebugEnabled() )
          LOG.debug( "adding edge: " + source + " -> " + current );

        addEdge( source, current ).setName( current.getName() ); // name scope after source
        }
      }

    for( Pipe previous : SubAssembly.unwind( current.getPrevious() ) )
      {
      makeGraph( previous, sources, sinks );

      if( LOG.isDebugEnabled() )
        LOG.debug( "adding edge: " + previous + " -> " + current );

      if( getEdge( previous, current ) != null )
        throw new ElementGraphException( previous, "cannot distinguish pipe branches, give pipe unique name: " + previous );

      addEdge( previous, current ).setName( previous.getName() ); // name scope after previous pipe
      }
    }

  /**
   * Method getTopologicalIterator returns the topologicalIterator of this ElementGraph object.
   *
   * @return the topologicalIterator (type TopologicalOrderIterator<FlowElement, Scope>) of this ElementGraph object.
   */
  public TopologicalOrderIterator<FlowElement, Scope> getTopologicalIterator()
    {
    return new TopologicalOrderIterator<FlowElement, Scope>( this );
    }

  /**
   * Method getAllShortestPathsFrom ...
   *
   * @param flowElement of type FlowElement
   * @return List<GraphPath<FlowElement, Scope>>
   */
  public List<GraphPath<FlowElement, Scope>> getAllShortestPathsFrom( FlowElement flowElement )
    {
    return getAllShortestPathsBetween( flowElement, tail );
    }

  /**
   * Method getAllShortestPathsTo ...
   *
   * @param flowElement of type FlowElement
   * @return List<GraphPath<FlowElement, Scope>>
   */
  public List<GraphPath<FlowElement, Scope>> getAllShortestPathsTo( FlowElement flowElement )
    {
    return getAllShortestPathsBetween( head, flowElement );
    }

  /**
   * Method getAllShortestPathsBetweenExtents returns the allShortestPathsBetweenExtents of this ElementGraph object.
   *
   * @return the allShortestPathsBetweenExtents (type List<GraphPath<FlowElement, Scope>>) of this ElementGraph object.
   */
  public List<GraphPath<FlowElement, Scope>> getAllShortestPathsBetweenExtents()
    {
    return new KShortestPaths<FlowElement, Scope>( this, head, Integer.MAX_VALUE ).getPaths( tail );
    }

  /**
   * Method getAllShortestPathsBetween ...
   *
   * @param from of type FlowElement
   * @param to   of type FlowElement
   * @return List<GraphPath<FlowElement, Scope>>
   */
  public List<GraphPath<FlowElement, Scope>> getAllShortestPathsBetween( FlowElement from, FlowElement to )
    {
    return new KShortestPaths<FlowElement, Scope>( this, from, Integer.MAX_VALUE ).getPaths( to );
    }

  /**
   * Method getDepthFirstIterator returns the depthFirstIterator of this ElementGraph object.
   *
   * @return the depthFirstIterator (type DepthFirstIterator<FlowElement, Scope>) of this ElementGraph object.
   */
  public DepthFirstIterator<FlowElement, Scope> getDepthFirstIterator()
    {
    return new DepthFirstIterator<FlowElement, Scope>( this, head );
    }

  private SimpleDirectedGraph<FlowElement, Scope> copyWithTraps()
    {
    ElementGraph copy = this.copyElementGraph();

    copy.addTraps();

    return copy;
    }

  private void addTraps()
    {
    DepthFirstIterator<FlowElement, Scope> iterator = getDepthFirstIterator();

    while( iterator.hasNext() )
      {
      FlowElement element = iterator.next();

      if( !( element instanceof Pipe ) )
        continue;

      Pipe pipe = (Pipe) element;
      Tap trap = traps.get( pipe.getName() );

      if( trap == null )
        continue;

      addVertex( trap );

      if( LOG.isDebugEnabled() )
        LOG.debug( "adding trap edge: " + pipe + " -> " + trap );

      if( getEdge( pipe, trap ) != null )
        continue;

      addEdge( pipe, trap ).setName( pipe.getName() ); // name scope after previous pipe
      }
    }

  /**
   * Method writeDOT writes this element graph to a DOT file for easy vizualization and debugging.
   *
   * @param filename of type String
   */
  public void writeDOT( String filename )
    {
    printElementGraph( filename, this.copyWithTraps() );
    }

  protected void printElementGraph( String filename, final SimpleDirectedGraph<FlowElement, Scope> graph )
    {
    try
      {
      Writer writer = new FileWriter( filename );

      Util.writeDOT( writer, graph, new IntegerNameProvider<FlowElement>(), new VertexNameProvider<FlowElement>()
      {
      public String getVertexName( FlowElement object )
        {
        if( object instanceof Tap || object instanceof Extent )
          return object.toString().replaceAll( "\"", "\'" );

        Scope scope = graph.outgoingEdgesOf( object ).iterator().next();

        return ( (Pipe) object ).print( scope ).replaceAll( "\"", "\'" );
        }
      }, new EdgeNameProvider<Scope>()
      {
      public String getEdgeName( Scope object )
        {
        return object.toString().replaceAll( "\"", "\'" ).replaceAll( "\n", "\\\\n" ); // fix for newlines in graphviz
        }
      } );

      writer.close();
      }
    catch( IOException exception )
      {
      exception.printStackTrace();
      }
    }

  /** Method removeEmptyPipes performs a depth first traversal and removes instance of {@link cascading.pipe.Pipe} or {@link cascading.pipe.SubAssembly}. */
  public void removeUnnecessaryPipes()
    {
    while( !internalRemoveUnnecessaryPipes() )
      ;

    int numPipes = 0;
    for( FlowElement flowElement : vertexSet() )
      {
      if( flowElement instanceof Pipe )
        numPipes++;
      }

    if( numPipes == 0 )
      throw new ElementGraphException( "resulting graph has no pipe elements after removing empty Pipe, assertions, and SubAssembly containers" );
    }

  private boolean internalRemoveUnnecessaryPipes()
    {
    DepthFirstIterator<FlowElement, Scope> iterator = getDepthFirstIterator();

    while( iterator.hasNext() )
      {
      FlowElement flowElement = iterator.next();

      if( flowElement.getClass() == Pipe.class || flowElement instanceof SubAssembly || testAssertion( flowElement, assertionLevel ) )
        {
        // Pipe class is guaranteed to have one input
        removeElement( flowElement );

        return false;
        }
      }

    return true;
    }

  private void removeElement( FlowElement flowElement )
    {
    LOG.debug( "removing: " + flowElement );

    Set<Scope> incomingScopes = incomingEdgesOf( flowElement );

    if( incomingScopes.size() != 1 )
      throw new IllegalStateException( "flow element:" + flowElement + ", has multiple input paths: " + incomingScopes.size() );

    Scope incoming = incomingScopes.iterator().next();
    Set<Scope> outgoingScopes = outgoingEdgesOf( flowElement );

    // source -> incoming -> flowElement -> outgoing -> target
    FlowElement source = getEdgeSource( incoming );

    for( Scope outgoing : outgoingScopes )
      {
      FlowElement target = getEdgeTarget( outgoing );

      addEdge( source, target, new Scope( outgoing ) );
      }

    removeVertex( flowElement );
    }

  private boolean testAssertion( FlowElement flowElement, AssertionLevel assertionLevel )
    {
    if( !( flowElement instanceof Operator ) )
      return false;

    Operator operator = (Operator) flowElement;

    return operator.isAssertion() && operator.getAssertionLevel().isStricterThan( assertionLevel );
    }

  /** Method resolveFields performs a breadth first traversal and resolves the tuple fields between each Pipe instance. */
  public void resolveFields()
    {
    if( resolved )
      throw new IllegalStateException( "element graph already resolved" );

    TopologicalOrderIterator<FlowElement, Scope> iterator = getTopologicalIterator();

    while( iterator.hasNext() )
      resolveFields( iterator.next() );

    resolved = true;
    }

  private void resolveFields( FlowElement source )
    {
    if( source instanceof Extent )
      return;

    Set<Scope> incomingScopes = incomingEdgesOf( source );
    Set<Scope> outgoingScopes = outgoingEdgesOf( source );

    List<FlowElement> flowElements = Graphs.successorListOf( this, source );

    if( flowElements.size() == 0 )
      throw new IllegalStateException( "unable to find next elements in pipeline from: " + source.toString() );

    Scope outgoingScope = source.outgoingScopeFor( incomingScopes );

    if( LOG.isDebugEnabled() )
      {
      LOG.debug( "for modifier: " + source );
      if( outgoingScope.getArgumentSelector() != null )
        LOG.debug( "setting outgoing arguments: " + outgoingScope.getArgumentSelector() );
      if( outgoingScope.getDeclaredFields() != null )
        LOG.debug( "setting outgoing declared: " + outgoingScope.getDeclaredFields() );
      if( outgoingScope.getGroupingSelectors() != null )
        LOG.debug( "setting outgoing group: " + outgoingScope.getGroupingSelectors() );
      if( outgoingScope.getOutValuesSelector() != null )
        LOG.debug( "setting outgoing values: " + outgoingScope.getOutValuesSelector() );
      }

    for( Scope scope : outgoingScopes )
      scope.copyFields( outgoingScope );
    }

  /**
   * Finds all groups that merge/join streams. returned in topological order.
   *
   * @return
   */
  public List<Group> findAllMergeJoinGroups()
    {
    return findAllOfType( 2, 1, Group.class, new LinkedList<Group>() );
    }

  /**
   * Method findAllGroups ...
   *
   * @return List<Group>
   */
  public List<Group> findAllGroups()
    {
    return findAllOfType( 1, 1, Group.class, new LinkedList<Group>() );
    }

  /**
   * Method findAllEveries ...
   *
   * @return List<Every>
   */
  public List<Every> findAllEveries()
    {
    return findAllOfType( 1, 1, Every.class, new LinkedList<Every>() );
    }

  /**
   * Method findAllTaps ...
   *
   * @return List<Tap>
   */
  public List<Tap> findAllTaps()
    {
    return findAllOfType( 1, 1, Tap.class, new LinkedList<Tap>() );
    }

  /**
   * Method findAllSplits ...
   *
   * @return List<FlowElement>
   */
  public List<Each> findAllEachSplits()
    {
    return findAllOfType( 1, 2, Each.class, new LinkedList<Each>() );
    }

  /**
   * Method findAllOfType ...
   *
   * @param minInDegree  of type int
   * @param minOutDegree
   * @param type         of type Class<P>
   * @param results      of type List<P>   @return List<P>
   */
  public <P> List<P> findAllOfType( int minInDegree, int minOutDegree, Class<P> type, List<P> results )
    {
    TopologicalOrderIterator<FlowElement, Scope> topoIterator = getTopologicalIterator();

    while( topoIterator.hasNext() )
      {
      FlowElement flowElement = topoIterator.next();

      if( type.isInstance( flowElement ) && inDegreeOf( flowElement ) >= minInDegree && outDegreeOf( flowElement ) >= minOutDegree )
        results.add( (P) flowElement );
      }

    return results;
    }

  public void insertFlowElementAfter( FlowElement previousElement, FlowElement flowElement )
    {
    Set<Scope> outgoing = new HashSet<Scope>( outgoingEdgesOf( previousElement ) );

    addVertex( flowElement );

    String name = previousElement.toString();

    if( previousElement instanceof Pipe )
      name = ( (Pipe) previousElement ).getName();

    addEdge( previousElement, flowElement, new Scope( name ) );

    for( Scope scope : outgoing )
      {
      FlowElement target = getEdgeTarget( scope );
      removeEdge( previousElement, target ); // remove scope
      addEdge( flowElement, target, scope ); // add scope back
      }
    }

  /** Simple class that acts in as the root of the graph */
  /**
   * Method makeTapGraph returns a directed graph of all taps in the current element graph.
   *
   * @return SimpleDirectedGraph<Tap, Integer>
   */
  public SimpleDirectedGraph<Tap, Integer> makeTapGraph()
    {
    SimpleDirectedGraph<Tap, Integer> tapGraph = new SimpleDirectedGraph<Tap, Integer>( Integer.class );
    List<GraphPath<FlowElement, Scope>> paths = getAllShortestPathsBetweenExtents();
    int count = 0;

    if( LOG.isDebugEnabled() )
      LOG.debug( "found num paths: " + paths.size() );

    for( GraphPath<FlowElement, Scope> element : paths )
      {
      List<Scope> path = element.getEdgeList();
      Tap lastTap = null;

      for( Scope scope : path )
        {
        FlowElement target = getEdgeTarget( scope );

        if( target instanceof Extent )
          continue;

        if( !( target instanceof Tap ) )
          continue;

        tapGraph.addVertex( (Tap) target );

        if( lastTap != null )
          {
          if( LOG.isDebugEnabled() )
            LOG.debug( "adding tap edge: " + lastTap + " -> " + target );

          if( tapGraph.getEdge( lastTap, (Tap) target ) == null && !tapGraph.addEdge( lastTap, (Tap) target, count++ ) )
            throw new ElementGraphException( "could not add graph edge: " + lastTap + " -> " + target );
          }

        lastTap = (Tap) target;
        }
      }

    return tapGraph;
    }

  public int getMaxNumPathsBetweenElementAndMergJoin( FlowElement flowElement )
    {
    List<Group> groups = findAllMergeJoinGroups();

    int maxPaths = 0;

    if( groups == null )
      return 0;

    for( Group group : groups )
      {
      if( flowElement != group )
        {
        List<GraphPath<FlowElement, Scope>> paths = getAllShortestPathsBetween( flowElement, group );

        if( paths != null )
          maxPaths = Math.max( maxPaths, paths.size() );
        }
      }

    return maxPaths;
    }

  public List<FlowElement> getAllSuccessors( FlowElement element )
    {
    return (List<FlowElement>) Graphs.successorListOf( this, element );
    }

  public void replaceElementWith( FlowElement element, FlowElement replacement )
    {
    Set<Scope> incoming = new HashSet<Scope>( incomingEdgesOf( element ) );
    Set<Scope> outgoing = new HashSet<Scope>( outgoingEdgesOf( element ) );

    if( !containsVertex( replacement ) )
      addVertex( replacement );

    for( Scope scope : incoming )
      {
      FlowElement source = getEdgeSource( scope );
      removeEdge( source, element ); // remove scope

      // drop edge between, if any
      if( source != replacement )
        addEdge( source, replacement, scope ); // add scope back
      }

    for( Scope scope : outgoing )
      {
      FlowElement target = getEdgeTarget( scope );
      removeEdge( element, target ); // remove scope

      // drop edge between, if any
      if( target != replacement )
        addEdge( replacement, target, scope ); // add scope back
      }

    removeVertex( element );
    }

  public <A extends FlowElement> Set<A> getAllChildrenOfType( FlowElement flowElement, Class<A> type )
    {
    Set<A> allChildren = new HashSet<A>();

    getAllChildrenOfType( allChildren, flowElement, type );

    return allChildren;
    }

  private <A extends FlowElement> void getAllChildrenOfType( Set<A> allSuccessors, FlowElement flowElement, Class<A> type )
    {
    List<FlowElement> sucessors = getAllSuccessors( flowElement );

    for( FlowElement sucessor : sucessors )
      {
      if( type.isInstance( sucessor ) )
        allSuccessors.add( (A) sucessor );
      else
        getAllChildrenOfType( allSuccessors, sucessor, type );
      }
    }

  public static class Extent extends Pipe
    {

    /** @see cascading.pipe.Pipe#Pipe(String) */
    public Extent( String name )
      {
      super( name );
      }

    /** @see cascading.pipe.Pipe#outgoingScopeFor(java.util.Set < cascading.flow.Scope>) */
    @Override
    public Scope outgoingScopeFor( Set<Scope> scopes )
      {
      return new Scope();
      }

    /** @see cascading.pipe.Pipe#toString() */
    @Override
    public String toString()
      {
      return "[" + getName() + "]";
      }

    public boolean equals( Object object )
      {
      if( object == null )
        return false;

      if( this == object )
        return true;

      if( object.getClass() != this.getClass() )
        return false;

      return this.getName().equals( ( (Pipe) object ).getName() );
      }
    }
  }
