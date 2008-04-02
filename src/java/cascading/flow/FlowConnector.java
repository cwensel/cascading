/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.pipe.EndPipe;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.pipe.PipeAssembly;
import cascading.tap.Tap;
import cascading.tap.TempHfs;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.alg.KShortestPaths;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.DepthFirstIterator;
import org.jgrapht.traverse.TopologicalOrderIterator;

/**
 * Use the FlowConnector to link sink and source {@link Tap} instances with an assembly of {@link Pipe} instances into
 * a {@link Flow}.
 */
public class FlowConnector
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( FlowConnector.class );

  /** Field head */
  private final Extent head = new Extent( "head" );
  /** Field tail */
  private final Extent tail = new Extent( "tail" );
  /** Field jobConf */
  private JobConf jobConf;

  /** Constructor FlowConnector creates a new FlowConnector instance. */
  public FlowConnector()
    {
    }

  /**
   * Constructor FlowConnector creates a new FlowConnector instance using the given {@link JobConf} instance as
   * default values for the underlying jobs.
   *
   * @param jobConf of type JobConf
   */
  public FlowConnector( JobConf jobConf )
    {
    this.jobConf = jobConf;
    }

  /**
   * Constructor FlowConnector creates a new FlowConnector instance using the given {@link Properties} instance as
   * default value for the underlying jobs. All properties are copied to a new {@link JobConf} instance.
   *
   * @param properties of type Properties
   */
  public FlowConnector( Properties properties )
    {
    copyProperties( properties );
    }

  private void copyProperties( Properties properties )
    {
    if( properties == null )
      return;

    jobConf = new JobConf();

    Enumeration enumeration = properties.propertyNames();

    while( enumeration.hasMoreElements() )
      {
      String key = (String) enumeration.nextElement();
      jobConf.set( key, properties.getProperty( key ) );
      }
    }

  /**
   * Method connect links the given source and sink Taps to the given pipe assembly.
   *
   * @param source of type Tap
   * @param sink   of type Tap
   * @param pipe   of type Pipe
   * @return Flow
   */
  public Flow connect( Tap source, Tap sink, Pipe pipe )
    {
    return connect( null, source, sink, pipe );
    }

  /**
   * Method connect links the given source and sink Taps to the given pipe assembly.
   *
   * @param name   of type String
   * @param source of type Tap
   * @param sink   of type Tap
   * @param pipe   of type Pipe
   * @return Flow
   */
  public Flow connect( String name, Tap source, Tap sink, Pipe pipe )
    {
    Map<String, Tap> sources = new HashMap<String, Tap>();

    sources.put( pipe.getHeads()[ 0 ].getName(), source );

    return connect( name, sources, sink, pipe );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   *
   * @param sources of type Map<String, Tap>
   * @param sink    of type Tap
   * @param pipe    of type Pipe
   * @return Flow
   */
  public Flow connect( Map<String, Tap> sources, Tap sink, Pipe pipe )
    {
    return connect( null, sources, sink, pipe );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   *
   * @param name    of type String
   * @param sources of type Map<String, Tap>
   * @param sink    of type Tap
   * @param pipe    of type Pipe
   * @return Flow
   */
  public Flow connect( String name, Map<String, Tap> sources, Tap sink, Pipe pipe )
    {
    Map<String, Tap> sinks = new HashMap<String, Tap>();

    sinks.put( pipe.getName(), sink );

    return connect( name, sources, sinks, pipe );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   * <p/>
   * Since only once source Tap is given, it is assumed to be associated with the 'head' pipe.
   * So the head pipe does not need to be included as an argument.
   *
   * @param source of type Tap
   * @param sinks  of type Map<String, Tap>
   * @param pipes  of type Pipe...
   * @return Flow
   */
  public Flow connect( Tap source, Map<String, Tap> sinks, Pipe... pipes )
    {
    return connect( null, source, sinks, pipes );
    }

  /**
   * Method connect links the named source Taps and sink Tap to the given pipe assembly.
   * <p/>
   * Since only once source Tap is given, it is assumed to be associated with the 'head' pipe.
   * So the head pipe does not need to be included as an argument.
   *
   * @param name   of type String
   * @param source of type Tap
   * @param sinks  of type Map<String, Tap>
   * @param pipes  of type Pipe...
   * @return Flow
   */
  public Flow connect( String name, Tap source, Map<String, Tap> sinks, Pipe... pipes )
    {
    Set<Pipe> heads = new HashSet<Pipe>();

    for( Pipe pipe : pipes )
      Collections.addAll( heads, pipe.getHeads() );

    if( heads.size() != 1 )
      throw new IllegalArgumentException( "there may be only 1 head pipe instance, found " + heads.size() );

    Map<String, Tap> sources = new HashMap<String, Tap>();

    for( Pipe pipe : heads )
      sources.put( pipe.getName(), source );

    return connect( name, sources, sinks, pipes );
    }

  /**
   * Method connect links the named sources and sinks to the given pipe assembly.
   *
   * @param sources of type Map<String, Tap>
   * @param sinks   of type Map<String, Tap>
   * @param pipes   of type Pipe...
   * @return Flow
   */
  public Flow connect( Map<String, Tap> sources, Map<String, Tap> sinks, Pipe... pipes )
    {
    return connect( null, sources, sinks, pipes );
    }

  /**
   * Method connect links the named sources and sinks to the given pipe assembly.
   *
   * @param name    of type String
   * @param sources of type Map<String, Tap>
   * @param sinks   of type Map<String, Tap>
   * @param pipes   of type Pipe...
   * @return Flow
   */
  public Flow connect( String name, Map<String, Tap> sources, Map<String, Tap> sinks, Pipe... pipes )
    {
    name = name == null ? makeName( pipes ) : name;

    return buildFlow( name, pipes, sources, sinks );
    }

  /**
   * Method buildFlow renders the actual Flow instance.
   *
   * @param name    of type String
   * @param pipes   of type Pipe[]
   * @param sources of type Map<String, Tap>
   * @param sinks   of type Map<String, Tap>
   * @return Flow
   */
  private Flow buildFlow( String name, Pipe[] pipes, Map<String, Tap> sources, Map<String, Tap> sinks )
    {
    SimpleDirectedGraph<FlowElement, Scope> pipeGraph = null;

    try
      {
      verifyTaps( sources, true );
      verifyTaps( sinks, false );

      verifyNames( sources, sinks, pipes );

      pipeGraph = makePipeGraph( pipes, sources, sinks );

      addExtents( pipeGraph, sources, sinks );
      verifyGraph( pipeGraph );

      handleSplits( pipeGraph );
      handleGroups( pipeGraph, sources.values() );
      removeEmptyPipes( pipeGraph ); // groups must be added before removing pipes
      resolveFields( pipeGraph );

      SimpleDirectedGraph<Tap, Integer> tapGraph = makeTapGraph( pipeGraph );
      SimpleDirectedGraph<FlowStep, Integer> stepGraph = makeStepGraph( pipeGraph, tapGraph );

      return new Flow( jobConf, name, pipeGraph, stepGraph, new HashMap<String, Tap>( sources ), new HashMap<String, Tap>( sinks ) );
      }
    catch( FlowException exception )
      {
      exception.pipeGraph = pipeGraph;

      throw exception;
      }
    catch( Exception exception )
      {
      // captures pipegraph for debugging
      throw new FlowException( "could not build flow from assembly", exception, pipeGraph );
      }
    }

  private void verifyTaps( Map<String, Tap> taps, boolean areSources )
    {
    for( String tapName : taps.keySet() )
      {
      if( areSources && !taps.get( tapName ).isSource() )
        throw new FlowException( "tap named: " + tapName + " is not a source: " + taps.get( tapName ) );
      else if( !areSources && !taps.get( tapName ).isSink() )
        throw new FlowException( "tap named: " + tapName + " is not a sink: " + taps.get( tapName ) );
      }
    }

  private void verifyNames( Map<String, Tap> sources, Map<String, Tap> sinks, Pipe[] pipes )
    {
    Set<String> names = new HashSet<String>();

    names.addAll( sources.keySet() );
    names.addAll( sinks.keySet() );

    for( Pipe pipe : pipes )
      {
      if( pipe instanceof PipeAssembly )
        {
        for( String tailName : ( (PipeAssembly) pipe ).getTailNames() )
          {
          if( !names.contains( tailName ) )
            throw new FlowException( "pipe name not found in either sink or source map: " + pipe.getName() );
          }
        }
      else if( !names.contains( pipe.getName() ) )
        {
        throw new FlowException( "pipe name not found in either sink or source map: " + pipe.getName() );
        }
      }

    for( Pipe pipe : pipes )
      {
      for( Pipe head : pipe.getHeads() )
        {
        if( !names.contains( head.getName() ) )
          throw new FlowException( "pipe name not found in either sink or source map: " + pipe.getName() );
        }
      }
    }

  private String makeName( Pipe[] pipes )
    {
    String[] names = new String[pipes.length];

    for( int i = 0; i < pipes.length; i++ )
      names[ i ] = pipes[ i ].getName();

    return Util.join( names, "+" );
    }

  /**
   * created to support the ability to generate all paths between the head and tail of the process.
   *
   * @param graph
   * @param sources
   * @param sinks
   */
  private void addExtents( SimpleDirectedGraph<FlowElement, Scope> graph, Map<String, Tap> sources, Map<String, Tap> sinks )
    {
    graph.addVertex( head );

    for( String source : sources.keySet() )
      {
      Scope scope = graph.addEdge( head, sources.get( source ) );

      // edge may already exist, if so, above returns null
      if( scope != null )
        scope.setName( source );
      }

    graph.addVertex( tail );

    for( String sink : sinks.keySet() )
      graph.addEdge( sinks.get( sink ), tail ).setName( sink );
    }

  private void verifyGraph( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    if( pipeGraph.vertexSet().isEmpty() )
      return;

    // need to verify that only EndPipe instances are origins in this graph. Otherwise a Tap was not properly connected
    TopologicalOrderIterator<FlowElement, Scope> iterator = new TopologicalOrderIterator<FlowElement, Scope>( pipeGraph );

    while( iterator.hasNext() )
      {
      FlowElement flowElement = iterator.next();

      if( pipeGraph.incomingEdgesOf( flowElement ).size() != 0 )
        break;

      if( flowElement instanceof Extent )
        continue;

      if( flowElement instanceof Pipe )
        throw new FlowException( "no Tap instance given to connect Pipe " + flowElement.toString() );
      else if( flowElement instanceof Tap )
        throw new FlowException( "no Pipe instance given to connect Tap " + flowElement.toString() );
      else
        throw new FlowException( "unknown element type: " + flowElement );
      }
    }

  /**
   * Method removeEmptyPipes performs a depth first traversal and removes instance of {@link Pipe} or {@link PipeAssembly}.
   *
   * @param graph of type SimpleDirectedGraph<FlowElement, Scope>
   */
  private void removeEmptyPipes( SimpleDirectedGraph<FlowElement, Scope> graph )
    {
    DepthFirstIterator<FlowElement, Scope> iterator = new DepthFirstIterator<FlowElement, Scope>( graph, head );
    Set<FlowElement> remove = new HashSet<FlowElement>();

    out:
    while( iterator.hasNext() )
      {
      FlowElement flowElement = iterator.next();

      if( flowElement.getClass() == Pipe.class || flowElement instanceof PipeAssembly )
        {
        if( LOG.isDebugEnabled() )
          LOG.debug( "remove testing pipe: " + flowElement );

        Set<Scope> incomingScopes = graph.incomingEdgesOf( flowElement ); // Pipe class is guaranteed to have one input
        Scope incoming = incomingScopes.iterator().next();
        Set<Scope> outgoingScopes = graph.outgoingEdgesOf( flowElement );

        // source -> incoming -> flowElement -> outgoing -> target
        FlowElement source = graph.getEdgeSource( incoming );
        for( Scope outgoing : outgoingScopes )
          {
          FlowElement target = graph.getEdgeTarget( outgoing );

          // don't connect two taps directly together
          if( source instanceof Tap && target instanceof Tap )
            {
            if( flowElement.getClass() == Pipe.class )
              throw new FlowException( "cannot connect source and sink with a only a Pipe class" );

            continue out;
            }

          graph.addEdge( source, target, new Scope( outgoing ) );
          }

        remove.add( flowElement );
        }
      }

    for( FlowElement flowElement : remove )
      {
      if( LOG.isDebugEnabled() )
        LOG.debug( "removing: " + flowElement );

      graph.removeVertex( flowElement );
      }
    }

  /**
   * Method resolveFields performs a breadth first traversal and resolves the tuple fields between each Pipe instance.
   *
   * @param graph of type SimpleDirectedGraph<FlowElement, Scope>
   */
  private void resolveFields( SimpleDirectedGraph<FlowElement, Scope> graph )
    {
    TopologicalOrderIterator<FlowElement, Scope> iterator = new TopologicalOrderIterator<FlowElement, Scope>( graph );

    while( iterator.hasNext() )
      resolveFields( graph, iterator.next() );
    }

  private void resolveFields( SimpleDirectedGraph<FlowElement, Scope> graph, FlowElement source )
    {
    if( source instanceof Extent )
      return;

    Set<Scope> incomingScopes = graph.incomingEdgesOf( source );
    Set<Scope> outgoingScopes = graph.outgoingEdgesOf( source );

    // don't bother with sink taps
    List<FlowElement> flowElements = Graphs.successorListOf( graph, source );

    if( flowElements.size() == 0 )
      throw new IllegalStateException( "unable to find next elements in pipeline from: " + source.toString() );

    if( flowElements.get( 0 ) instanceof Extent )
      return;

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
   * optimized for this case
   * <pre>
   *        e - t
   * t - e -
   *        e - t
   * </pre>
   * <p/>
   * this should run in two map/red jobs, not 3
   *
   * @param pipeGraph
   */
  private void handleSplits( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    // copy so we can modify the graph while iterating
    DepthFirstIterator<FlowElement, Scope> iterator = new DepthFirstIterator<FlowElement, Scope>( copyPipeGraph( pipeGraph ), head );

    FlowElement lastInsertable = null;

    while( iterator.hasNext() )
      {
      FlowElement flowElement = iterator.next();

      if( flowElement instanceof Extent )
        continue;

      // if Tap, Group, or Every - we insert the tap here
      if( flowElement instanceof Tap || flowElement instanceof Group || flowElement instanceof Every )
        lastInsertable = flowElement;

      if( flowElement instanceof Tap )
        continue;
      else if( pipeGraph.outDegreeOf( flowElement ) <= 1 )
        continue;

      if( !( lastInsertable instanceof Tap ) )
        insertTapAfter( pipeGraph, (Pipe) lastInsertable );
      }
    }

  /**
   * Method copyPipeGraph creates a copy of the given pipeGraph.
   *
   * @param pipeGraph of type SimpleDirectedGraph<FlowElement, Scope>
   * @return SimpleDirectedGraph<FlowElement, Scope>
   */
  private SimpleDirectedGraph<FlowElement, Scope> copyPipeGraph( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    SimpleDirectedGraph<FlowElement, Scope> copy = new SimpleDirectedGraph<FlowElement, Scope>( Scope.class );
    Graphs.addGraph( copy, pipeGraph );
    return copy;
    }

  /**
   * Since all joins are at groups, depth first search is safe
   *
   * @param pipeGraph of type SimpleDirectedGraph<FlowElement, Scope>
   * @param sources   of type Collection<Tap>
   */
  private void handleGroups( SimpleDirectedGraph<FlowElement, Scope> pipeGraph, Collection<Tap> sources )
    {
    // if there was a graph change, iterate paths again. prevents many temp taps from being inserted infront of a group
    while( !handleGroupsInternal( pipeGraph, sources ) )
      ;
    }

  private boolean handleGroupsInternal( SimpleDirectedGraph<FlowElement, Scope> pipeGraph, Collection<Tap> sources )
    {
    KShortestPaths<FlowElement, Scope> shortestPaths = new KShortestPaths<FlowElement, Scope>( pipeGraph, head, Integer.MAX_VALUE );
    List<GraphPath<FlowElement, Scope>> paths = shortestPaths.getPaths( tail );

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      Iterator<Scope> scopeIterator = path.getEdgeList().iterator();

      List<Pipe> tapInsertions = new ArrayList<Pipe>();

      boolean foundGroup = false;

      FlowElement previousFlowElement = null;
      FlowElement flowElement = head;
      while( scopeIterator.hasNext() )
        {
        previousFlowElement = flowElement;
        flowElement = pipeGraph.getEdgeTarget( scopeIterator.next() );

        if( flowElement instanceof Extent )
          continue;
        else if( flowElement instanceof Tap && sources.contains( (Tap) flowElement ) )
          continue;

        if( flowElement instanceof Group && !foundGroup )
          foundGroup = true;
        else if( flowElement instanceof Group && foundGroup )
          tapInsertions.add( (Pipe) previousFlowElement );
        else if( flowElement instanceof Tap )
          foundGroup = false;
        }

      for( Pipe pipe : tapInsertions )
        insertTapAfter( pipeGraph, pipe );

      if( !tapInsertions.isEmpty() )
        return false;
      }

    return true;
    }

  private void insertTapAfter( SimpleDirectedGraph<FlowElement, Scope> graph, Pipe pipe )
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "inserting tap after: " + pipe );

    TempHfs tempDfs = makeTemp( pipe );
    Set<Scope> outgoing = new HashSet<Scope>( graph.outgoingEdgesOf( pipe ) );

    graph.addVertex( tempDfs );
    graph.addEdge( pipe, tempDfs, new Scope( pipe.getName() ) );

    for( Scope scope : outgoing )
      {
      FlowElement target = graph.getEdgeTarget( scope );
      graph.removeEdge( pipe, target ); // remove scope
      graph.addEdge( tempDfs, target, scope ); // add scope back
      }
    }

  private TempHfs makeTemp( Pipe pipe )
    {
    // must give Taps unique names
    return new TempHfs( pipe.getName().replace( ' ', '_' ) + "/" + (int) ( Math.random() * 100000 ) + "/" );
    }

  private SimpleDirectedGraph<FlowElement, Scope> makePipeGraph( Pipe[] pipes, Map<String, Tap> sources, Map<String, Tap> sinks )
    {
    SimpleDirectedGraph<FlowElement, Scope> graph = new SimpleDirectedGraph<FlowElement, Scope>( Scope.class );
    HashMap<String, Tap> sourcesCopy = new HashMap<String, Tap>( sources );
    HashMap<String, Tap> sinksCopy = new HashMap<String, Tap>( sinks );

    for( Pipe pipe : pipes )
      makePipeGraph( graph, pipe, sourcesCopy, sinksCopy );

    return graph;
    }

  /**
   * Perfoming no rule checks here.
   * <p/>
   * Scopes are always named after the source side of the source -> target relationship
   *
   * @param current
   * @param sources
   * @param sinks
   */
  private void makePipeGraph( SimpleDirectedGraph<FlowElement, Scope> graph, Pipe current, Map<String, Tap> sources, Map<String, Tap> sinks )
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "adding pipe: " + current );

    if( current instanceof PipeAssembly )
      {
      for( Pipe pipe : current.getPrevious() )
        makePipeGraph( graph, pipe, sources, sinks );

      return;
      }

    if( graph.containsVertex( current ) )
      return;

    graph.addVertex( current );

    Tap sink = sinks.remove( current.getName() );

    if( sink != null )
      {
      if( LOG.isDebugEnabled() )
        LOG.debug( "adding sink: " + sink );

      graph.addVertex( sink );

      if( LOG.isDebugEnabled() )
        LOG.debug( "adding edge: " + current + " -> " + sink );

      graph.addEdge( current, sink ).setName( current.getName() ); // name scope after sink
      }

    if( current.getPrevious().length == 0 )
      {
      Tap source = sources.remove( current.getName() );

      if( source != null )
        {
        if( LOG.isDebugEnabled() )
          LOG.debug( "adding source: " + source );

        graph.addVertex( source );

        if( LOG.isDebugEnabled() )
          LOG.debug( "adding edge: " + source + " -> " + current );

        graph.addEdge( source, current ).setName( current.getName() ); // name scope after source
        }
      }

    for( Pipe previous : current.getPrevious() )
      {
      makePipeGraph( graph, previous, sources, sinks );

      if( LOG.isDebugEnabled() )
        LOG.debug( "adding edge: " + previous + " -> " + current );

      if( previous instanceof PipeAssembly )
        {
        // handle PipeAssembly instances, they are not part of the graph
        for( Pipe pipe : previous.getPrevious() )
          graph.addEdge( pipe, current ).setName( pipe.getName() ); // name scope after previous pipe
        }
      else
        {
        graph.addEdge( previous, current ).setName( previous.getName() ); // name scope after previous pipe
        }
      }
    }

  private SimpleDirectedGraph<Tap, Integer> makeTapGraph( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    SimpleDirectedGraph<Tap, Integer> tapGraph = new SimpleDirectedGraph<Tap, Integer>( Integer.class );
    KShortestPaths<FlowElement, Scope> shortestPaths = new KShortestPaths<FlowElement, Scope>( pipeGraph, head, Integer.MAX_VALUE );
    List<GraphPath<FlowElement, Scope>> paths = shortestPaths.getPaths( tail );
    int count = 0;

    if( LOG.isDebugEnabled() )
      LOG.debug( "found num paths: " + paths.size() );

    for( GraphPath<FlowElement, Scope> element : paths )
      {
      List<Scope> path = element.getEdgeList();
      Tap lastTap = null;

      for( Scope scope : path )
        {
        FlowElement target = pipeGraph.getEdgeTarget( scope );

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
            throw new FlowException( "could not add graph edge: " + lastTap + " -> " + target );
          }

        lastTap = (Tap) target;
        }
      }

    return tapGraph;
    }

  private SimpleDirectedGraph<FlowStep, Integer> makeStepGraph( SimpleDirectedGraph<FlowElement, Scope> pipeGraph, SimpleDirectedGraph<Tap, Integer> tapGraph )
    {
    Map<String, FlowStep> steps = new LinkedHashMap<String, FlowStep>();
    SimpleDirectedGraph<FlowStep, Integer> stepGraph = new SimpleDirectedGraph<FlowStep, Integer>( Integer.class );
    TopologicalOrderIterator<Tap, Integer> topoIterator = new TopologicalOrderIterator<Tap, Integer>( tapGraph );
    int count = 0;

    while( topoIterator.hasNext() )
      {
      FlowElement source = topoIterator.next();

      if( LOG.isDebugEnabled() )
        LOG.debug( "handling source: " + source );

      List<Tap> sinks = Graphs.successorListOf( tapGraph, (Tap) source );

      for( Tap sink : sinks )
        {
        if( LOG.isDebugEnabled() )
          LOG.debug( "handling path: " + source + " -> " + sink );

        FlowStep step = getCreateFlowStep( steps, sink.toString() );

        stepGraph.addVertex( step );

        if( steps.containsKey( source.toString() ) )
          stepGraph.addEdge( steps.get( source.toString() ), step, count++ );

        List<Scope> scopes = DijkstraShortestPath.findPathBetween( pipeGraph, source, sink );
        String sourceName = scopes.get( 0 ).getName();

        step.sources.put( (Tap) source, sourceName );
        step.sink = sink;

        if( step.sink.isUseTapCollector() || Graphs.predecessorListOf( pipeGraph, sink ).get( 0 ) instanceof EndPipe )
          step.tempSink = new TempHfs( sink.getPath().toUri().getPath() );

        FlowElement lhs = source;

        step.graph.addVertex( lhs );

        for( Scope scope : scopes )
          {
          FlowElement rhs = pipeGraph.getEdgeTarget( scope );

          step.graph.addVertex( rhs );
          step.graph.addEdge( lhs, rhs, scope );

          if( rhs instanceof Group )
            step.group = (Group) rhs;

          lhs = rhs;
          }
        }
      }

    return stepGraph;
    }

  private FlowStep getCreateFlowStep( Map<String, FlowStep> steps, String name )
    {
    if( steps.containsKey( name ) )
      return steps.get( name );

    if( LOG.isDebugEnabled() )
      LOG.debug( "creating step: " + name );

    FlowStep step = new FlowStep( name );

    steps.put( name, step );

    return step;
    }

  /** Simple class that acts in as the root of the graph */
  static class Extent extends Pipe
    {
    /** @see Pipe#Pipe(String) */
    public Extent( String name )
      {
      super( name );
      }

    /** @see Pipe#outgoingScopeFor(Set<Scope>) */
    @Override
    public Scope outgoingScopeFor( Set<Scope> scopes )
      {
      return new Scope();
      }

    /** @see Pipe#toString() */
    @Override
    public String toString()
      {
      return "[" + getName() + "]";
      }
    }
  }
