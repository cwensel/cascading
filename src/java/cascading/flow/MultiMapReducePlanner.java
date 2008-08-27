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
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.hadoop.HadoopUtil;
import cascading.operation.AssertionLevel;
import cascading.pipe.Each;
import cascading.pipe.EndPipe;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
import cascading.tap.TempHfs;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
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
   * Method setNormalizeHeterogeneousSources adds the given doNormalize boolean to the given properites object.
   * Use this method if additional jobs should be planned in to handle incompatible InputFormat classes.
   * <p/>
   * Normalization is off by default.
   *
   * @param properties  of type Map
   * @param doNormalize of type boolean
   */
  public static void setNormalizeHeterogeneousSources( Map<Object, Object> properties, boolean doNormalize )
    {
    properties.put( "cascading.multimapreduceplanner.normalizesources", Boolean.toString( doNormalize ) );
    }

  /**
   * Method getNormalizeHeterogeneousSources returns if this planner will normalize heterogeneous input sources.
   *
   * @param properties of type Map
   * @return a boolean
   */
  public static boolean getNormalizeHeterogeneousSources( Map<Object, Object> properties )
    {
    return Util.getProperty( properties, "cascading.multimapreduceplanner.normalizesources", false );
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
    ElementGraph elementGraph = null;

    try
      {
      // generic
      verifyTaps( sources, true, true );
      verifyTaps( sinks, false, true );
      verifyTaps( traps, false, false );

      verifyPipeAssemblyEndPoints( sources, sinks, pipes );
      verifyTraps( traps, pipes );

      elementGraph = new ElementGraph( pipes, sources, sinks, assertionLevel );

      // rules
      failOnLoneGroupAssertion( elementGraph );
      failOnMissingGroup( elementGraph );

      // m/r specific
      handleSplits( elementGraph );
      handleGroups( elementGraph );

      if( getNormalizeHeterogeneousSources( properties ) )
        handleHeterogeneousSources( elementGraph );

      // generic
      elementGraph.removeUnnecessaryPipes(); // groups must be added before removing pipes
      elementGraph.resolveFields();

      // m/r specific
      SimpleDirectedGraph<Tap, Integer> tapGraph = makeTapGraph( elementGraph );
      SimpleDirectedGraph<FlowStep, Integer> stepGraph = makeStepGraph( elementGraph, tapGraph, traps );

      // clone data
      sources = new HashMap<String, Tap>( sources );
      sinks = new HashMap<String, Tap>( sinks );
      traps = new HashMap<String, Tap>( traps );

      return new Flow( properties, jobConf, name, elementGraph, stepGraph, sources, sinks, traps );
      }
    catch( PlannerException exception )
      {
      exception.elementGraph = elementGraph;

      throw exception;
      }
    catch( Exception exception )
      {
      // captures pipegraph for debugging
      // forward message in case cause or trace is lost
      String message = String.format( "could not build flow from assembly: [%s]", exception.getMessage() );
      throw new PlannerException( message, exception, elementGraph );
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

      collectNames( SubAssembly.unwind( pipe.getPrevious() ), names );
      }
    }

  /**
   * Verifies that there are not only GroupAssertions following any given Group instance. This will adversely
   * affect the stream entering any subsquent Tap of Each instances.
   *
   * @param elementGraph
   */
  private void failOnLoneGroupAssertion( ElementGraph elementGraph )
    {
    List<Group> groups = elementGraph.findAllGroups();

    // walk Every instances after Group
    for( Group group : groups )
      {
      for( GraphPath<FlowElement, Scope> path : elementGraph.getAllShortestPathsFrom( group ) )
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

  private void failOnMissingGroup( ElementGraph elementGraph )
    {
    List<Every> everies = elementGraph.findAllEveries();

    // walk Every instances after Group
    for( Every every : everies )
      {
      for( GraphPath<FlowElement, Scope> path : elementGraph.getAllShortestPathsTo( every ) )
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
   * @param elementGraph
   */
  private void handleSplits( ElementGraph elementGraph )
    {
    // copy so we can modify the graph while iterating
    DepthFirstIterator<FlowElement, Scope> iterator = new DepthFirstIterator<FlowElement, Scope>( elementGraph.copyElementGraph(), elementGraph.head );

    FlowElement lastInsertable = null;

    while( iterator.hasNext() )
      {
      FlowElement flowElement = iterator.next();

      if( flowElement instanceof ElementGraph.Extent )
        continue;

      // if Tap, Group, or Every - we insert the tap here
      if( flowElement instanceof Tap || flowElement instanceof Group || flowElement instanceof Every )
        lastInsertable = flowElement;

      if( flowElement instanceof Tap )
        continue;
      else if( elementGraph.outDegreeOf( flowElement ) <= 1 )
        continue;

      if( lastInsertable instanceof Tap )
        continue;

      insertTapAfter( elementGraph, (Pipe) flowElement );
      }
    }

  private void handleHeterogeneousSources( ElementGraph elementGraph )
    {
    while( !internalHeterogeneousSources( elementGraph ) )
      ;
    }

  private boolean internalHeterogeneousSources( ElementGraph elementGraph )
    {
    // find all Groups
    List<Group> groups = elementGraph.findAllMergeJoinGroups();

    // compare group sources
    Map<Group, Set<Tap>> normalizeGroups = new HashMap<Group, Set<Tap>>();

    for( Group group : groups )
      {
      KShortestPaths<FlowElement, Scope> shortestPaths = new KShortestPaths<FlowElement, Scope>( elementGraph, elementGraph.head, Integer.MAX_VALUE );
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
        if( tap instanceof TempHfs || getSchemeClass( tap ).equals( intermediateSchemeClass ) ) // we normalize to TempHfs
          continue;

        KShortestPaths<FlowElement, Scope> shortestPaths = new KShortestPaths<FlowElement, Scope>( elementGraph, tap, Integer.MAX_VALUE );
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

          insertTapAfter( elementGraph, (Pipe) flowElement );

          return false;
          }
        }
      }

    return normalizeGroups.isEmpty();
    }

  private Class getSchemeClass( Tap tap )
    {
    if( tap instanceof TempHfs )
      return ( (TempHfs) tap ).getSchemeClass();
    else
      return tap.getScheme().getClass();
    }

  /**
   * Since all joins are at groups, depth first search is safe
   *
   * @param elementGraph of type PipeGraph
   */
  private void handleGroups( ElementGraph elementGraph )
    {
    // if there was a graph change, iterate paths again. prevents many temp taps from being inserted infront of a group
    while( !handleGroupPartitioning( elementGraph ) )
      ;
    }

  private boolean handleGroupPartitioning( ElementGraph elementGraph )
    {
    KShortestPaths<FlowElement, Scope> shortestPaths = new KShortestPaths<FlowElement, Scope>( elementGraph, elementGraph.head, Integer.MAX_VALUE );
    List<GraphPath<FlowElement, Scope>> paths = shortestPaths.getPaths( elementGraph.tail );

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      List<FlowElement> flowElements = Graphs.getPathVertexList( path );
      List<Pipe> tapInsertions = new ArrayList<Pipe>();

      boolean foundGroup = false;

      for( int i = 0; i < flowElements.size(); i++ )
        {
        FlowElement flowElement = flowElements.get( i );

        if( flowElement instanceof ElementGraph.Extent ) // is an extent: head or tail
          continue;
        else
          if( flowElement instanceof Tap && flowElements.get( i - 1 ) instanceof ElementGraph.Extent )  // is a source tap
            continue;

        if( flowElement instanceof Group && !foundGroup )
          foundGroup = true;
        else if( flowElement instanceof Group && foundGroup ) // add tap between groups
          tapInsertions.add( (Pipe) flowElements.get( i - 1 ) );
        else if( flowElement instanceof Tap )
          foundGroup = false;
        }

      for( Pipe pipe : tapInsertions )
        insertTapAfter( elementGraph, pipe );

      if( !tapInsertions.isEmpty() )
        return false;
      }

    return true;
    }

  /**
   * Method insertTapAfter ...
   *
   * @param graph of type PipeGraph
   * @param pipe  of type Pipe
   */
  private void insertTapAfter( ElementGraph graph, Pipe pipe )
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "inserting tap after: " + pipe );

    TempHfs tempDfs = makeTemp( pipe );

    insertFlowElementAfter( graph, pipe, tempDfs );
    }

  private void insertFlowElementAfter( ElementGraph graph, FlowElement previousElement, FlowElement flowElement )
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
   * Method makeTapGraph ...
   *
   * @param elementGraph of type PipeGraph
   * @return SimpleDirectedGraph<Tap, Integer>
   */
  private SimpleDirectedGraph<Tap, Integer> makeTapGraph( ElementGraph elementGraph )
    {
    SimpleDirectedGraph<Tap, Integer> tapGraph = new SimpleDirectedGraph<Tap, Integer>( Integer.class );
    List<GraphPath<FlowElement, Scope>> paths = elementGraph.getAllShortestPathsBetweenExtents();
    int count = 0;

    if( LOG.isDebugEnabled() )
      LOG.debug( "found num paths: " + paths.size() );

    for( GraphPath<FlowElement, Scope> element : paths )
      {
      List<Scope> path = element.getEdgeList();
      Tap lastTap = null;

      for( Scope scope : path )
        {
        FlowElement target = elementGraph.getEdgeTarget( scope );

        if( target instanceof ElementGraph.Extent )
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
   * @param elementGraph of type PipeGraph
   * @param tapGraph     of type SimpleDirectedGraph<Tap, Integer>
   * @param traps        of type Map<String, Tap>
   * @return SimpleDirectedGraph<FlowStep, Integer>
   */
  private SimpleDirectedGraph<FlowStep, Integer> makeStepGraph( ElementGraph elementGraph, SimpleDirectedGraph<Tap, Integer> tapGraph, Map<String, Tap> traps )
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

        // support multiple paths from source to sink
        // this allows for self joins on groups, even with different operation stacks between them
        // note we must ignore paths with intermediate taps
        List<GraphPath<FlowElement, Scope>> paths = elementGraph.getAllShortestPathsBetween( source, sink );

        for( GraphPath<FlowElement, Scope> path : paths )
          {
          if( pathContainsTap( path ) )
            continue;

          List<Scope> scopes = path.getEdgeList();
          String sourceName = scopes.get( 0 ).getName(); // root node of the shortest path

          step.sources.put( (Tap) source, sourceName );
          step.sink = sink;

          if( step.sink.isUseTapCollector() || Graphs.predecessorListOf( elementGraph, sink ).get( 0 ) instanceof EndPipe )
            step.tempSink = new TempHfs( sink.getPath().toUri().getPath() );

          FlowElement lhs = source;

          step.graph.addVertex( lhs );

          for( Scope scope : scopes )
            {
            FlowElement rhs = elementGraph.getEdgeTarget( scope );

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
      }

    return stepGraph;
    }

  private boolean pathContainsTap( GraphPath<FlowElement, Scope> path )
    {
    List<FlowElement> flowElements = Graphs.getPathVertexList( path );

    // first and last are taps, if we find more than 2, return false
    int count = 0;

    for( FlowElement flowElement : flowElements )
      {
      if( flowElement instanceof Tap )
        count++;
      }

    return count > 2;
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

  }
