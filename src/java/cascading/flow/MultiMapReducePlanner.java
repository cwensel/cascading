/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.hadoop.HadoopUtil;
import cascading.operation.AssertionLevel;
import cascading.pipe.Each;
import cascading.pipe.EndPipe;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.Operator;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
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
 * Class MultiMapReducePlanner is the core Hadoop MapReduce planner.
 * <p/>
 * Notes:
 * <p/>
 * <strong>Custom JobConf properties</strong><br/>
 * A custo JobConf instance can be passed to this planner by calling {@link #setJobConf(java.util.Map, org.apache.hadoop.mapred.JobConf)}
 * on a map properties object before constructing a new {@link FlowConnector}.
 * <p/>
 * A better practice would be to set Hadoop properties directly on the map properties object handed to the FlowConnector.
 * All values in the map will be passed to a new default JobConf instance to be used as defaults for all resulting
 * Flow instances.
 * <p/>
 * For example, {@code properties.set("mapred.child.java.opts","-Xmx512m");} would convince Hadoop
 * to spawn all child jvms with a heap of 512MB.
 * <p/>
 * <strong>Heterogeneous source Tap instances</strong><br/>
 * Currently Hadoop cannot have but one InputFormat per Mapper, but Cascading allows for any types of Taps
 * to be used as sinks in a given Flow.
 * <p/>
 * To overcome this issue, this planner will insert temporary Tap
 * instances immediately before a merge or join Group (GroupBy or CoGroup) if the source Taps do not share
 * the same Scheme class. By default temp Taps use the SequenceFile Scheme. So if the source Taps are custom
 * or use TextLine, a few extra jobs can leak into a given Flow.
 * <p/>
 * To overcome this, in turn, an intermediateSchemeClass must be passed from the FlowConnctor to the planner. This class
 * will be instantiated for every temp Tap instance. The intention is that the given intermedeiateSchemeClass
 * match all the source Tap schemes.
 * <p/>
 * <strong>Properties</strong><br/>
 * <ul>
 * <li>cascading.hadoop.jobconf</li>
 * </ul>
 */
public class MultiMapReducePlanner
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( MultiMapReducePlanner.class );

  /** Field head */
  private final Extent head = new Extent( "head" );
  /** Field tail */
  private final Extent tail = new Extent( "tail" );

  /** Field properties */
  private final Map<Object, Object> properties;

  /** Field jobConf */
  private JobConf jobConf;
  /** Field assertionLevel */
  private AssertionLevel assertionLevel;
  /** Field intermediateSchemeClass */
  private final Class intermediateSchemeClass;

  /**
   * Method setJobConf adds the given JobConf object to the given properties object. Use this method to pass
   * custom default Hadoop JobConf properties to Hadoop.
   *
   * @param properties of type Map
   * @param jobConf    of type JobConf
   */
  public static void setJobConf( Map<Object, Object> properties, JobConf jobConf )
    {
    properties.put( "cascading.hadoop.jobconf", jobConf );
    }

  /**
   * Method getJobConf returns a stored JobConf instance, if any.
   *
   * @param properties of type Map
   * @return a JobConf instance
   */
  public static JobConf getJobConf( Map<Object, Object> properties )
    {
    return Util.getProperty( properties, "cascading.hadoop.jobconf", (JobConf) null );
    }

  /**
   * Constructor MultiMapReducePlanner creates a new MultiMapReducePlanner instance.
   *
   * @param properties of type Map<Object, Object>
   */
  protected MultiMapReducePlanner( Map<Object, Object> properties )
    {
    this.properties = properties;
    this.jobConf = getJobConf( properties );
    this.jobConf = HadoopUtil.createJobConf( properties, this.jobConf );
    this.assertionLevel = FlowConnector.getAssertionLevel( properties );
    this.intermediateSchemeClass = FlowConnector.getIntermediateSchemeClass( properties );
    }

  /**
   * Method buildFlow renders the actual Flow instance.
   *
   * @param name    of type String
   * @param pipes   of type Pipe[]
   * @param sources of type Map<String, Tap>
   * @param sinks   of type Map<String, Tap>
   * @param traps   of type Map<String, Tap>
   * @return Flow
   */
  public Flow buildFlow( String name, Pipe[] pipes, Map<String, Tap> sources, Map<String, Tap> sinks, Map<String, Tap> traps )
    {
    SimpleDirectedGraph<FlowElement, Scope> pipeGraph = null;

    try
      {
      // generic
      verifyTaps( sources, true, true );
      verifyTaps( sinks, false, true );
      verifyTaps( traps, false, false );

      verifyPipeAssemblyEndPoints( sources, sinks, pipes );
      verifyTraps( traps, pipes );

      pipeGraph = makePipeGraph( pipes, sources, sinks );

      addExtents( pipeGraph, sources, sinks );
      verifyGraphConnections( pipeGraph );

      failOnLoneGroupAssertion( pipeGraph );
      failOnMissingGroup( pipeGraph );

      // m/r specific
      handleSplits( pipeGraph );
      handleGroups( pipeGraph );
      handleHeterogeneousSources( pipeGraph );

      // generic
      verifyUniqueGroupSources( pipeGraph );
      removeUnnecessaryPipes( pipeGraph ); // groups must be added before removing pipes
      resolveFields( pipeGraph );

      // m/r specific
      SimpleDirectedGraph<Tap, Integer> tapGraph = makeTapGraph( pipeGraph );
      SimpleDirectedGraph<FlowStep, Integer> stepGraph = makeStepGraph( pipeGraph, tapGraph, traps );

      return new Flow( properties, jobConf, name, pipeGraph, stepGraph, new HashMap<String, Tap>( sources ), new HashMap<String, Tap>( sinks ), new HashMap<String, Tap>( traps ) );
      }
    catch( PlannerException exception )
      {
      exception.pipeGraph = pipeGraph;

      throw exception;
      }
    catch( Exception exception )
      {
      // captures pipegraph for debugging
      // forward message in case cause or trace is lost
      String message = String.format( "could not build flow from assembly: [%s]", exception.getMessage() );
      throw new PlannerException( message, exception, pipeGraph );
      }
    }

  /**
   * Method verifyTaps ...
   *
   * @param taps          of type Map<String, Tap>
   * @param areSources    of type boolean
   * @param mayNotBeEmpty of type boolean
   */
  private void verifyTaps( Map<String, Tap> taps, boolean areSources, boolean mayNotBeEmpty )
    {
    if( mayNotBeEmpty && taps.isEmpty() )
      throw new PlannerException( ( areSources ? "source" : "sink" ) + " taps are required" );

    for( String tapName : taps.keySet() )
      {
      if( areSources && !taps.get( tapName ).isSource() )
        throw new PlannerException( "tap named: " + tapName + " is not a source: " + taps.get( tapName ) );
      else if( !areSources && !taps.get( tapName ).isSink() )
        throw new PlannerException( "tap named: " + tapName + " is not a sink: " + taps.get( tapName ) );
      }
    }

  /**
   * Method verifyEndPoints ...
   *
   * @param sources of type Map<String, Tap>
   * @param sinks   of type Map<String, Tap>
   * @param pipes   of type Pipe[]
   */
  private void verifyPipeAssemblyEndPoints( Map<String, Tap> sources, Map<String, Tap> sinks, Pipe[] pipes )
    {
    Set<String> names = new HashSet<String>();

    names.addAll( sources.keySet() );
    names.addAll( sinks.keySet() );

    // handle tails
    for( Pipe pipe : pipes )
      {
      if( pipe instanceof SubAssembly )
        {
        for( String tailName : ( (SubAssembly) pipe ).getTailNames() )
          {
          if( !names.contains( tailName ) )
            throw new PlannerException( "pipe name not found in either sink or source map: " + tailName );
          }
        }
      else if( !names.contains( pipe.getName() ) )
        {
        throw new PlannerException( "pipe name not found in either sink or source map: " + pipe.getName() );
        }
      }

    // handle heads
    for( Pipe pipe : pipes )
      {
      for( Pipe head : pipe.getHeads() )
        {
        if( !names.contains( head.getName() ) )
          throw new PlannerException( "pipe name not found in either sink or source map: " + head.getName() );
        }
      }
    }

  /**
   * Method verifyTraps ...
   *
   * @param traps of type Map<String, Tap>
   * @param pipes of type Pipe[]
   */
  private void verifyTraps( Map<String, Tap> traps, Pipe[] pipes )
    {
    Set<String> names = new HashSet<String>();

    collectNames( pipes, names );

    for( String name : traps.keySet() )
      {
      if( !names.contains( name ) )
        throw new PlannerException( "trap name not found in assembly: " + name );
      }
    }

  /**
   * Method collectNames ...
   *
   * @param pipes of type Pipe[]
   * @param names of type Set<String>
   */
  private void collectNames( Pipe[] pipes, Set<String> names )
    {
    for( Pipe pipe : pipes )
      {
      if( pipe instanceof SubAssembly )
        names.addAll( Arrays.asList( ( (SubAssembly) pipe ).getTailNames() ) );
      else
        names.add( pipe.getName() );

      collectNames( unwindPipeAssemblies( pipe.getPrevious() ), names );
      }
    }

  /**
   * created to support the ability to generate all paths between the head and tail of the process.
   *
   * @param pipeGraph
   * @param sources
   * @param sinks
   */
  private void addExtents( SimpleDirectedGraph<FlowElement, Scope> pipeGraph, Map<String, Tap> sources, Map<String, Tap> sinks )
    {
    pipeGraph.addVertex( head );

    for( String source : sources.keySet() )
      {
      Scope scope = pipeGraph.addEdge( head, sources.get( source ) );

      // edge may already exist, if so, above returns null
      if( scope != null )
        scope.setName( source );
      }

    pipeGraph.addVertex( tail );

    for( String sink : sinks.keySet() )
      pipeGraph.addEdge( sinks.get( sink ), tail ).setName( sink );
    }

  /**
   * Method verifyGraphConnections ...
   *
   * @param pipeGraph of type SimpleDirectedGraph<FlowElement, Scope>
   */
  private void verifyGraphConnections( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
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
        throw new PlannerException( "no Tap instance given to connect Pipe " + flowElement.toString() );
      else if( flowElement instanceof Tap )
        throw new PlannerException( "no Pipe instance given to connect Tap " + flowElement.toString() );
      else
        throw new PlannerException( "unknown element type: " + flowElement );
      }
    }

  /**
   * Verifies that there are not only GroupAssertions following any given Group instance. This will adversely
   * affect the stream entering any subsquent Tap of Each instances.
   *
   * @param pipeGraph
   */
  private void failOnLoneGroupAssertion( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    List<Group> groups = findAllGroups( pipeGraph );

    // walk Every instances after Group
    for( Group group : groups )
      {
      KShortestPaths<FlowElement, Scope> shortestPaths = new KShortestPaths<FlowElement, Scope>( pipeGraph, group, Integer.MAX_VALUE );
      List<GraphPath<FlowElement, Scope>> paths = shortestPaths.getPaths( tail );

      for( GraphPath<FlowElement, Scope> path : paths )
        {
        List<FlowElement> flowElements = Graphs.getPathVertexList( path ); // last element is tail

        int everies = 0;
        int assertions = 0;

        for( FlowElement flowElement : flowElements )
          {
          if( flowElement instanceof Group )
            continue;

          if( !( flowElement instanceof Every ) )
            break;

          everies++;

          Every every = (Every) flowElement;

          if( every.getAssertionLevel() != null )
            assertions++;
          }

        if( everies != 0 && everies == assertions )
          throw new PlannerException( "group assertions must be accompanied by aggregator operations" );
        }
      }
    }

  private void failOnMissingGroup( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    List<Every> everies = findAllEveries( pipeGraph );

    // walk Every instances after Group
    for( Every every : everies )
      {
      KShortestPaths<FlowElement, Scope> shortestPaths = new KShortestPaths<FlowElement, Scope>( pipeGraph, head, Integer.MAX_VALUE );
      List<GraphPath<FlowElement, Scope>> paths = shortestPaths.getPaths( every );

      for( GraphPath<FlowElement, Scope> path : paths )
        {
        List<FlowElement> flowElements = Graphs.getPathVertexList( path ); // last element is every
        Collections.reverse( flowElements ); // first element is every

        for( FlowElement flowElement : flowElements )
          {
          if( flowElement instanceof Each )
            throw new PlannerException( "Every may only be preceeded by another Every or a Group pipe, found: " + flowElement );

          if( flowElement instanceof Every )
            continue;

          if( flowElement instanceof Group )
            break;
          }
        }
      }
    }

  /**
   * Method removeEmptyPipes performs a depth first traversal and removes instance of {@link Pipe} or {@link cascading.pipe.SubAssembly}.
   *
   * @param graph of type SimpleDirectedGraph<FlowElement, Scope>
   */
  private void removeUnnecessaryPipes( SimpleDirectedGraph<FlowElement, Scope> graph )
    {
    while( !internalRemoveUnnecessaryPipes( graph ) )
      ;
    }

  private boolean internalRemoveUnnecessaryPipes( SimpleDirectedGraph<FlowElement, Scope> graph )
    {
    DepthFirstIterator<FlowElement, Scope> iterator = new DepthFirstIterator<FlowElement, Scope>( graph, head );

    while( iterator.hasNext() )
      {
      FlowElement flowElement = iterator.next();

      if( flowElement.getClass() == Pipe.class || flowElement instanceof SubAssembly || testAssertion( flowElement ) )
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

          graph.addEdge( source, target, new Scope( outgoing ) );
          }

        if( LOG.isDebugEnabled() )
          LOG.debug( "removing: " + flowElement );

        graph.removeVertex( flowElement );

        return false;
        }
      }

    return true;
    }

  /**
   * Method testAssertion ...
   *
   * @param flowElement of type FlowElement
   * @return boolean
   */
  private boolean testAssertion( FlowElement flowElement )
    {
    if( !( flowElement instanceof Operator ) )
      return false;

    Operator operator = (Operator) flowElement;

    return operator.isAssertion() && operator.getAssertionLevel().isStricterThan( assertionLevel );
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

  /**
   * Method resolveFields ...
   *
   * @param graph  of type SimpleDirectedGraph<FlowElement, Scope>
   * @param source of type FlowElement
   */
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
   *         e - t           e1 - e - t
   * t - e1 -       -- > t -
   *         e - t           e1 - e - t
   * </pre>
   * <p/>
   * this should run in two map/red jobs, not 3. needs to be a flag on e1 to prevent this
   * <p/>
   * <pre>
   *        g - t                 g - t
   * g - e -       --> g - e - t -
   *        g - t                 g - t
   * </pre>
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

      if( lastInsertable instanceof Tap )
        continue;

      insertTapAfter( pipeGraph, (Pipe) flowElement );
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

  private void handleHeterogeneousSources( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    while( !internalHeterogeneousSources( pipeGraph ) )
      ;
    }

  private boolean internalHeterogeneousSources( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    // find all Groups
    List<Group> groups = findAllMergeJoinGroups( pipeGraph );

    // compare group sources
    Map<Group, Set<Tap>> normalizeGroups = new HashMap<Group, Set<Tap>>();

    for( Group group : groups )
      {
      KShortestPaths<FlowElement, Scope> shortestPaths = new KShortestPaths<FlowElement, Scope>( pipeGraph, head, Integer.MAX_VALUE );
      List<GraphPath<FlowElement, Scope>> paths = shortestPaths.getPaths( group );

      Set<Tap> taps = new HashSet<Tap>();

      // iterate each shortest path to current group finding each tap sourcing the merge/join
      for( GraphPath<FlowElement, Scope> path : paths )
        {
        List<FlowElement> flowElements = Graphs.getPathVertexList( path ); // last element is group
        Collections.reverse( flowElements ); // first element is group

        for( FlowElement previousElement : flowElements )
          {
          if( previousElement instanceof Tap )
            {
            taps.add( (Tap) previousElement );
            break; // stop finding taps in this path
            }
          }
        }

      if( taps.size() == 1 )
        continue;

      Iterator<Tap> iterator = taps.iterator();
      Tap commonTap = iterator.next();

      while( iterator.hasNext() )
        {
        Tap tap = iterator.next();

        // making assumption hadoop can handle multiple filesytems, but not multiple inputformats
        // in the same job
        // possibly could test for common input format
        if( getSchemeClass( tap ) != getSchemeClass( commonTap ) )
          {
          normalizeGroups.put( group, taps );
          break;
          }
        }
      }

    // if incompatible, insert Tap after its join/merge pipe
    for( Group group : normalizeGroups.keySet() )
      {
      Set<Tap> taps = normalizeGroups.get( group );

      for( Tap tap : taps )
        {
        if( tap instanceof TempHfs ) // we normalize to TempHfs
          continue;

        KShortestPaths<FlowElement, Scope> shortestPaths = new KShortestPaths<FlowElement, Scope>( pipeGraph, tap, Integer.MAX_VALUE );
        List<GraphPath<FlowElement, Scope>> paths = shortestPaths.getPaths( group );

        // handle case where there is a split on a pipe between the tap and group
        for( GraphPath<FlowElement, Scope> path : paths )
          {
          List<FlowElement> flowElements = Graphs.getPathVertexList( path ); // shortest path tap -> group
          Collections.reverse( flowElements ); // group -> tap

          FlowElement flowElement = flowElements.get( 1 );

          if( flowElement instanceof TempHfs )
            continue;

          LOG.warn( "inserting step to normalize incompatible sources: " + tap );

          insertTapAfter( pipeGraph, (Pipe) flowElement );

          return false;
          }
        }
      }

    return normalizeGroups.isEmpty();
    }

  /**
   * Finds all groups that merge/join streams. returned in topological order.
   *
   * @param pipeGraph
   * @return
   */
  private List<Group> findAllMergeJoinGroups( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    return findAllOfType( pipeGraph, 2, Group.class, new LinkedList<Group>() );
    }

  private List<Group> findAllGroups( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    return findAllOfType( pipeGraph, 1, Group.class, new LinkedList<Group>() );
    }

  private List<Every> findAllEveries( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    return findAllOfType( pipeGraph, 1, Every.class, new LinkedList<Every>() );
    }

  private <P> List<P> findAllOfType( SimpleDirectedGraph<FlowElement, Scope> pipeGraph, int minInDegree, Class<P> type, List<P> results )
    {
    TopologicalOrderIterator<FlowElement, Scope> topoIterator = new TopologicalOrderIterator<FlowElement, Scope>( pipeGraph );

    while( topoIterator.hasNext() )
      {
      FlowElement flowElement = topoIterator.next();

      if( type.isInstance( flowElement ) && pipeGraph.inDegreeOf( flowElement ) >= minInDegree )
        results.add( (P) flowElement );
      }

    return results;
    }

  private Class getSchemeClass( Tap tap )
    {
    if( tap instanceof TempHfs )
      return ( (TempHfs) tap ).getSchemeClass();
    else
      return tap.getScheme().getClass();
    }

  private void verifyUniqueGroupSources( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    List<Group> groups = findAllMergeJoinGroups( pipeGraph );

    SimpleDirectedGraph<FlowElement, Scope> reverseGraph = new SimpleDirectedGraph<FlowElement, Scope>( Scope.class );
    Graphs.addGraphReversed( reverseGraph, pipeGraph );

    for( Group group : groups )
      {
      KShortestPaths<FlowElement, Scope> shortestPaths = new KShortestPaths<FlowElement, Scope>( reverseGraph, group, Integer.MAX_VALUE );
      List<GraphPath<FlowElement, Scope>> paths = shortestPaths.getPaths( head );

      Set<Tap> taps = new HashSet<Tap>();

      int incoming = pipeGraph.inDegreeOf( group );

      for( GraphPath<FlowElement, Scope> path : paths )
        {
        for( FlowElement flowElement : Graphs.getPathVertexList( path ) )
          {
          if( !( flowElement instanceof Tap ) )
            continue;

          Tap tap = (Tap) flowElement;

          taps.add( tap );
          break;
          }
        }

      if( taps.size() != incoming )
        throw new PlannerException( "groups may not join/merge duplicate sources, found incoming: " + Util.join( taps, ", " ) );
      }
    }

  /**
   * Since all joins are at groups, depth first search is safe
   *
   * @param pipeGraph of type SimpleDirectedGraph<FlowElement, Scope>
   */
  private void handleGroups( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    // if there was a graph change, iterate paths again. prevents many temp taps from being inserted infront of a group
    while( !handleGroupPartitioning( pipeGraph ) )
      ;
    }

  private boolean handleGroupPartitioning( SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    KShortestPaths<FlowElement, Scope> shortestPaths = new KShortestPaths<FlowElement, Scope>( pipeGraph, head, Integer.MAX_VALUE );
    List<GraphPath<FlowElement, Scope>> paths = shortestPaths.getPaths( tail );

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      List<FlowElement> flowElements = Graphs.getPathVertexList( path );
      List<Pipe> tapInsertions = new ArrayList<Pipe>();

      boolean foundGroup = false;

      for( int i = 0; i < flowElements.size(); i++ )
        {
        FlowElement flowElement = flowElements.get( i );

        if( flowElement instanceof Extent ) // is an extent: head or tail
          continue;
        else if( flowElement instanceof Tap && flowElements.get( i - 1 ) instanceof Extent )  // is a source tap
          continue;

        if( flowElement instanceof Group && !foundGroup )
          foundGroup = true;
        else if( flowElement instanceof Group && foundGroup ) // add tap between groups
          tapInsertions.add( (Pipe) flowElements.get( i - 1 ) );
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

  /**
   * Method insertTapAfter ...
   *
   * @param graph of type SimpleDirectedGraph<FlowElement, Scope>
   * @param pipe  of type Pipe
   */
  private void insertTapAfter( SimpleDirectedGraph<FlowElement, Scope> graph, Pipe pipe )
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "inserting tap after: " + pipe );

    TempHfs tempDfs = makeTemp( pipe );

    insertFlowElementAfter( graph, pipe, tempDfs );
    }

  private void insertFlowElementAfter( SimpleDirectedGraph<FlowElement, Scope> graph, FlowElement previousElement, FlowElement flowElement )
    {
    Set<Scope> outgoing = new HashSet<Scope>( graph.outgoingEdgesOf( previousElement ) );

    graph.addVertex( flowElement );

    String name = previousElement.toString();

    if( previousElement instanceof Pipe )
      name = ( (Pipe) previousElement ).getName();

    graph.addEdge( previousElement, flowElement, new Scope( name ) );

    for( Scope scope : outgoing )
      {
      FlowElement target = graph.getEdgeTarget( scope );
      graph.removeEdge( previousElement, target ); // remove scope
      graph.addEdge( flowElement, target, scope ); // add scope back
      }
    }

  /**
   * Method makeTemp ...
   *
   * @param pipe of type Pipe
   * @return TempHfs
   */
  private TempHfs makeTemp( Pipe pipe )
    {
    // must give Taps unique names
    return new TempHfs( pipe.getName().replace( ' ', '_' ) + "/" + (int) ( Math.random() * 100000 ) + "/", intermediateSchemeClass );
    }

  /**
   * Method makePipeGraph ...
   *
   * @param pipes   of type Pipe[]
   * @param sources of type Map<String, Tap>
   * @param sinks   of type Map<String, Tap>
   * @return SimpleDirectedGraph<FlowElement, Scope>
   */
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
   * Perfoms one rule check, verifies group does not join duplicate tap resources.
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

    if( current instanceof SubAssembly )
      {
      for( Pipe pipe : unwindPipeAssemblies( current.getPrevious() ) )
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

    // PipeAssemblies should always have a previous
    if( unwindPipeAssemblies( current.getPrevious() ).length == 0 )
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

    for( Pipe previous : unwindPipeAssemblies( current.getPrevious() ) )
      {
      makePipeGraph( graph, previous, sources, sinks );

      if( LOG.isDebugEnabled() )
        LOG.debug( "adding edge: " + previous + " -> " + current );

      addEdge( graph, current, previous );
      }
    }

  private void addEdge( SimpleDirectedGraph<FlowElement, Scope> graph, Pipe current, Pipe previous )
    {
    if( graph.getEdge( previous, current ) != null )
      throw new FlowException( "cannot distinguish pipe branches, give pipe unique name: " + previous );

    graph.addEdge( previous, current ).setName( previous.getName() ); // name scope after previous pipe
    }


  /**
   * Is responsible for unwinding nested PipeAssembly instances.
   *
   * @param tails
   * @return
   */
  private Pipe[] unwindPipeAssemblies( Pipe... tails )
    {
    Set<Pipe> previous = new HashSet<Pipe>();

    for( Pipe pipe : tails )
      {
      if( pipe instanceof SubAssembly )
        Collections.addAll( previous, unwindPipeAssemblies( pipe.getPrevious() ) );
      else
        previous.add( pipe );
      }

    return previous.toArray( new Pipe[previous.size()] );
    }

  /**
   * Method makeTapGraph ...
   *
   * @param pipeGraph of type SimpleDirectedGraph<FlowElement, Scope>
   * @return SimpleDirectedGraph<Tap, Integer>
   */
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
            throw new PlannerException( "could not add graph edge: " + lastTap + " -> " + target );
          }

        lastTap = (Tap) target;
        }
      }

    return tapGraph;
    }

  /**
   * Method makeStepGraph ...
   *
   * @param pipeGraph of type SimpleDirectedGraph<FlowElement, Scope>
   * @param tapGraph  of type SimpleDirectedGraph<Tap, Integer>
   * @param traps     of type Map<String, Tap>
   * @return SimpleDirectedGraph<FlowStep, Integer>
   */
  private SimpleDirectedGraph<FlowStep, Integer> makeStepGraph( SimpleDirectedGraph<FlowElement, Scope> pipeGraph, SimpleDirectedGraph<Tap, Integer> tapGraph, Map<String, Tap> traps )
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
        String sourceName = scopes.get( 0 ).getName(); // root node of the shortest path

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

          if( rhs instanceof Pipe ) // add relevant traps to step
            {
            String name = ( (Pipe) rhs ).getName();

            if( traps.containsKey( name ) )
              step.traps.put( name, traps.get( name ) );
            }

          lhs = rhs;
          }
        }
      }

    return stepGraph;
    }

  /**
   * Method getCreateFlowStep ...
   *
   * @param steps of type Map<String, FlowStep>
   * @param name  of type String
   * @return FlowStep
   */
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
