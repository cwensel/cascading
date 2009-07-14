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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.hadoop.HadoopUtil;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.OperatorException;
import cascading.pipe.Pipe;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tap.TempHfs;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.KShortestPaths;

/**
 * Class MultiMapReducePlanner is the core Hadoop MapReduce planner.
 * <p/>
 * Notes:
 * <p/>
 * <strong>Custom JobConf properties</strong><br/>
 * A custom JobConf instance can be passed to this planner by calling {@link #setJobConf(java.util.Map, org.apache.hadoop.mapred.JobConf)}
 * on a map properties object before constructing a new {@link FlowConnector}.
 * <p/>
 * A better practice would be to set Hadoop properties directly on the map properties object handed to the FlowConnector.
 * All values in the map will be passed to a new default JobConf instance to be used as defaults for all resulting
 * Flow instances.
 * <p/>
 * For example, {@code properties.set("mapred.child.java.opts","-Xmx512m");} would convince Hadoop
 * to spawn all child jvms with a heap of 512MB.
 * <p/>
 * <strong>Properties</strong><br/>
 * <ul>
 * <li>cascading.hadoop.jobconf</li>
 * <li>cascading.multimapreduceplanner.job.status.pollinterval</li>
 * </ul>
 */
public class MultiMapReducePlanner extends FlowPlanner
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( MultiMapReducePlanner.class );

  /** Field jobConf */
  private JobConf jobConf;
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
   * Normalization is off by default and should only be enabled by advanced users. Typically this will decrease
   * application performance.
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
    super( properties );
    jobConf = HadoopUtil.createJobConf( properties, getJobConf( properties ) );
    intermediateSchemeClass = FlowConnector.getIntermediateSchemeClass( properties );

    Class type = FlowConnector.getApplicationJarClass( properties );
    if( jobConf.getJar() == null && type != null )
      jobConf.setJarByClass( type );

    String path = FlowConnector.getApplicationJarPath( properties );
    if( jobConf.getJar() == null && path != null )
      jobConf.setJar( path );

    if( jobConf.getJar() == null )
      jobConf.setJarByClass( Util.findMainClass( MultiMapReducePlanner.class ) );

    LOG.info( "using application jar: " + jobConf.getJar() );
    }

  /**
   * Method buildFlow renders the actual Flow instance.
   *
   * @param flowName of type String
   * @param pipes    of type Pipe[]
   * @param sources  of type Map<String, Tap>
   * @param sinks    of type Map<String, Tap>
   * @param traps    of type Map<String, Tap>
   * @return Flow
   */
  public Flow buildFlow( String flowName, Pipe[] pipes, Map<String, Tap> sources, Map<String, Tap> sinks, Map<String, Tap> traps )
    {
    ElementGraph elementGraph = null;

    try
      {
      // generic
      verifyAssembly( pipes, sources, sinks, traps );

      elementGraph = createElementGraph( pipes, sources, sinks, traps );

      // rules
      failOnLoneGroupAssertion( elementGraph );
      failOnMissingGroup( elementGraph );
      failOnMisusedBuffer( elementGraph );

      // m/r specific
      handleSplit( elementGraph );
      handleGroupPartitioning( elementGraph );
      handleNonSafeOperations( elementGraph );

      if( getNormalizeHeterogeneousSources( properties ) )
        handleHeterogeneousSources( elementGraph );

      // generic
      elementGraph.removeUnnecessaryPipes(); // groups must be added before removing pipes
      elementGraph.resolveFields();

      // m/r specific
      handleAdjacentTaps( elementGraph );

      StepGraph stepGraph = new StepGraph( flowName, elementGraph, traps );

      // clone data
      sources = new HashMap<String, Tap>( sources );
      sinks = new HashMap<String, Tap>( sinks );
      traps = new HashMap<String, Tap>( traps );

      return new Flow( properties, jobConf, flowName, elementGraph, stepGraph, sources, sinks, traps );
      }
    catch( PlannerException exception )
      {
      exception.elementGraph = elementGraph;

      throw exception;
      }
    catch( ElementGraphException exception )
      {
      Throwable cause = exception.getCause();

      if( cause == null )
        cause = exception;

      // captures pipegraph for debugging
      // forward message in case cause or trace is lost
      String message = String.format( "could not build flow from assembly: [%s]", cause.getMessage() );

      if( cause instanceof OperatorException )
        throw new PlannerException( message, cause, elementGraph );

      throw new PlannerException( exception.getPipe(), message, cause, elementGraph );
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
   * <p/>
   * <pre>
   *             - e - e                            e - e
   * t - e1 - e2         - g  --> t - e1 - e2 - t -       - g
   *             - e - e                            e - e
   * </pre>
   *
   * @param elementGraph
   */
  private void handleSplit( ElementGraph elementGraph )
    {
    // if there was a graph change, iterate paths again.
    while( !internalSplit( elementGraph ) )
      ;
    }

  private boolean internalSplit( ElementGraph elementGraph )
    {
    List<GraphPath<FlowElement, Scope>> paths = elementGraph.getAllShortestPathsBetweenExtents();

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      List<FlowElement> flowElements = Graphs.getPathVertexList( path );
      Set<Pipe> tapInsertions = new HashSet<Pipe>();
      FlowElement lastInsertable = null;

      for( int i = 0; i < flowElements.size(); i++ )
        {
        FlowElement flowElement = flowElements.get( i );

        if( flowElement instanceof ElementGraph.Extent ) // is an extent: head or tail
          continue;

        // if Tap, Group, or Every - we insert the tap here
        if( flowElement instanceof Tap || flowElement instanceof Group || flowElement instanceof Every )
          lastInsertable = flowElement;

        // support splits on Pipe unless the previous is a Tap
        if( flowElement.getClass() == Pipe.class && flowElements.get( i - 1 ) instanceof Tap )
          continue;

        if( flowElement instanceof Tap )
          continue;

        if( elementGraph.outDegreeOf( flowElement ) <= 1 )
          continue;

        // we are at the root of a split here

        // do any split paths converge on a single Group?
        int maxPaths = elementGraph.getMaxNumPathsBetweenElementAndMergJoin( flowElement );
        if( maxPaths <= 1 && lastInsertable instanceof Tap )
          continue;

        tapInsertions.add( (Pipe) flowElement );
        }

      for( Pipe pipe : tapInsertions )
        insertTempTapAfter( elementGraph, pipe );

      if( !tapInsertions.isEmpty() )
        return false;
      }

    return true;
    }

  private void handleNonSafeOperations( ElementGraph elementGraph )
    {
    // if there was a graph change, iterate paths again.
    while( !internalNonSafeOperations( elementGraph ) )
      ;
    }

  private boolean internalNonSafeOperations( ElementGraph elementGraph )
    {
    Set<Pipe> tapInsertions = new HashSet<Pipe>();

    List<Each> splits = elementGraph.findAllEachSplits();

    // if any predecessor is safe, insert temp
    for( Each split : splits )
      {
      List<GraphPath<FlowElement, Scope>> paths = elementGraph.getAllShortestPathsTo( split );

      for( GraphPath<FlowElement, Scope> path : paths )
        {
        List<FlowElement> elements = Graphs.getPathVertexList( path );
        Collections.reverse( elements );

        for( FlowElement element : elements )
          {
          if( !( element instanceof Each ) )
            break;

          if( !( (Each) element ).getOperation().isSafe() )
            {
            tapInsertions.add( split );
            break;
            }
          }
        }
      }

    for( Pipe pipe : tapInsertions )
      insertTempTapAfter( elementGraph, pipe );

    return tapInsertions.isEmpty();
    }

  /**
   * will collapse adjacent and equivalent taps.
   * equivalence is based on the tap adjacent taps using the same filesystem
   * and the sink being symetrical, and having the same fields as the temp tap.
   * <p/>
   * <p/>
   * must be run after fields are resolved so temp taps have fully defined scheme instances.
   *
   * @param elementGraph
   */
  private void handleAdjacentTaps( ElementGraph elementGraph )
    {
    // if there was a graph change, iterate paths again.
    while( !internalAdjacentTaps( elementGraph ) )
      ;
    }

  private boolean internalAdjacentTaps( ElementGraph elementGraph )
    {
    List<Tap> taps = elementGraph.findAllTaps();

    for( Tap tap : taps )
      {
      if( !( tap instanceof TempHfs ) )
        continue;

      for( FlowElement successor : elementGraph.getAllSuccessors( tap ) )
        {
        if( !( successor instanceof Hfs ) )
          continue;

        Hfs successorTap = (Hfs) successor;

        // does this scheme source what it sinks
        if( !successorTap.getScheme().isSymetrical() )
          continue;

        URI tempURIScheme = getDefaultURIScheme( tap ); // temp uses default fs
        URI successorURIScheme = getURIScheme( successorTap );

        if( !tempURIScheme.equals( successorURIScheme ) )
          continue;

        // safe, both are symetrical
        if( !tap.getScheme().getSourceFields().equals( successorTap.getScheme().getSourceFields() ) )
          continue;

        elementGraph.replaceElementWith( tap, successor );

        return false;
        }
      }

    return true;
    }

  private URI getDefaultURIScheme( Tap tap )
    {
    try
      {
      return ( (Hfs) tap ).getDefaultFileSystemURIScheme( jobConf );
      }
    catch( IOException exception )
      {
      throw new PlannerException( "unable to get default URI scheme from tap: " + tap );
      }
    }

  private URI getURIScheme( Tap tap )
    {
    try
      {
      return ( (Hfs) tap ).getURIScheme( jobConf );
      }
    catch( IOException exception )
      {
      throw new PlannerException( "unable to get URI scheme from tap: " + tap );
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

          insertTempTapAfter( elementGraph, (Pipe) flowElement );

          return false;
          }
        }
      }

    return normalizeGroups.isEmpty();
    }

  /**
   * Since all joins are at groups, depth first search is safe
   *
   * @param elementGraph of type PipeGraph
   */
  private void handleGroupPartitioning( ElementGraph elementGraph )
    {
    // if there was a graph change, iterate paths again. prevents many temp taps from being inserted infront of a group
    while( !internalGroupPartitioning( elementGraph ) )
      ;
    }

  private boolean internalGroupPartitioning( ElementGraph elementGraph )
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
        else if( flowElement instanceof Tap && flowElements.get( i - 1 ) instanceof ElementGraph.Extent )  // is a source tap
          continue;

        if( flowElement instanceof Group && !foundGroup )
          foundGroup = true;
        else if( flowElement instanceof Group && foundGroup ) // add tap between groups
          tapInsertions.add( (Pipe) flowElements.get( i - 1 ) );
        else if( flowElement instanceof Tap )
            foundGroup = false;
        }

      for( Pipe pipe : tapInsertions )
        insertTempTapAfter( elementGraph, pipe );

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
  void insertTempTapAfter( ElementGraph graph, Pipe pipe )
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "inserting tap after: " + pipe );

    graph.insertFlowElementAfter( pipe, makeTemp( pipe ) );
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
    String name = pipe.getName();

    name = name.substring( 0, name.length() < 25 ? name.length() : 25 ).replaceAll( "\\s+|\\*|\\+", "_" );

    return new TempHfs( name + "/" + (int) ( Math.random() * 100000 ) + "/", intermediateSchemeClass );
    }

  private Class getSchemeClass( Tap tap )
    {
    if( tap instanceof TempHfs )
      return ( (TempHfs) tap ).getSchemeClass();
    else
      return tap.getScheme().getClass();
    }
  }
