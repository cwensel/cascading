/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop.planner;

import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowElement;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.ElementGraph;
import cascading.flow.planner.ElementGraphs;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.FlowStepGraph;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.Scope;
import cascading.pipe.CoGroup;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.property.PropertyUtil;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.util.TempHfs;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.planner.ElementGraphs.getAllShortestPathsBetween;

/**
 * Class HadoopPlanner is the core Hadoop MapReduce planner.
 * <p/>
 * Notes:
 * <p/>
 * <strong>Custom JobConf properties</strong><br/>
 * A custom JobConf instance can be passed to this planner by calling {@link #copyJobConf(java.util.Map, org.apache.hadoop.mapred.JobConf)}
 * on a map properties object before constructing a new {@link cascading.flow.hadoop.HadoopFlowConnector}.
 * <p/>
 * A better practice would be to set Hadoop properties directly on the map properties object handed to the FlowConnector.
 * All values in the map will be passed to a new default JobConf instance to be used as defaults for all resulting
 * Flow instances.
 * <p/>
 * For example, {@code properties.set("mapred.child.java.opts","-Xmx512m");} would convince Hadoop
 * to spawn all child jvms with a heap of 512MB.
 */
public class HadoopPlanner extends FlowPlanner<HadoopFlow, JobConf>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HadoopPlanner.class );

  /** Field jobConf */
  private JobConf jobConf;
  /** Field intermediateSchemeClass */
  private Class intermediateSchemeClass;

  /**
   * Method copyJobConf adds the given JobConf values to the given properties object. Use this method to pass
   * custom default Hadoop JobConf properties to Hadoop.
   *
   * @param properties of type Map
   * @param jobConf    of type JobConf
   */
  public static void copyJobConf( Map<Object, Object> properties, JobConf jobConf )
    {
    for( Map.Entry<String, String> entry : jobConf )
      properties.put( entry.getKey(), entry.getValue() );
    }

  /**
   * Method createJobConf returns a new JobConf instance using the values in the given properties argument.
   *
   * @param properties of type Map
   * @return a JobConf instance
   */
  public static JobConf createJobConf( Map<Object, Object> properties )
    {
    JobConf conf = new JobConf();

    copyProperties( conf, properties );

    return conf;
    }

  /**
   * Method copyProperties adds the given Map values to the given JobConf object.
   *
   * @param jobConf    of type JobConf
   * @param properties of type Map
   */
  public static void copyProperties( JobConf jobConf, Map<Object, Object> properties )
    {
    if( properties instanceof Properties )
      {
      Properties props = (Properties) properties;
      Set<String> keys = props.stringPropertyNames();

      for( String key : keys )
        jobConf.set( key, props.getProperty( key ) );
      }
    else
      {
      for( Map.Entry<Object, Object> entry : properties.entrySet() )
        {
        if( entry.getValue() != null )
          jobConf.set( entry.getKey().toString(), entry.getValue().toString() );
        }
      }
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
    return Boolean.parseBoolean( PropertyUtil.getProperty( properties, "cascading.multimapreduceplanner.normalizesources", "false" ) );
    }

  @Override
  public JobConf getConfig()
    {
    return jobConf;
    }

  @Override
  public PlatformInfo getPlatformInfo()
    {
    return HadoopUtil.getPlatformInfo();
    }

  @Override
  public void initialize( FlowConnector flowConnector, Map<Object, Object> properties )
    {
    super.initialize( flowConnector, properties );

    jobConf = HadoopUtil.createJobConf( properties, createJobConf( properties ) );
    intermediateSchemeClass = flowConnector.getIntermediateSchemeClass( properties );

    Class type = AppProps.getApplicationJarClass( properties );
    if( jobConf.getJar() == null && type != null )
      jobConf.setJarByClass( type );

    String path = AppProps.getApplicationJarPath( properties );
    if( jobConf.getJar() == null && path != null )
      jobConf.setJar( path );

    if( jobConf.getJar() == null )
      jobConf.setJarByClass( HadoopUtil.findMainClass( HadoopPlanner.class ) );

    AppProps.setApplicationJarPath( properties, jobConf.getJar() );

    LOG.info( "using application jar: {}", jobConf.getJar() );
    }

  @Override
  protected HadoopFlow createFlow( FlowDef flowDef )
    {
    return new HadoopFlow( getPlatformInfo(), getProperties(), getConfig(), flowDef );
    }

  @Override
  public HadoopFlow buildFlow( FlowDef flowDef )
    {
    ElementGraph elementGraph = null;

    try
      {
      // generic
      verifyAllTaps( flowDef );

      HadoopFlow flow = createFlow( flowDef );

      Pipe[] tails = resolveTails( flowDef, flow );

      verifyAssembly( flowDef, tails );

      elementGraph = createElementGraph( flowDef, tails );

      // rules
      failOnLoneGroupAssertion( elementGraph );
      failOnMissingGroup( elementGraph );
      failOnMisusedBuffer( elementGraph );
      failOnGroupEverySplit( elementGraph );

      // m/r specific
      handleWarnEquivalentPaths( elementGraph );
      handleSplit( elementGraph );
      handleJobPartitioning( elementGraph );
      handleJoins( elementGraph );
      handleNonSafeOperations( elementGraph );

      if( getNormalizeHeterogeneousSources( properties ) )
        handleHeterogeneousSources( elementGraph );

      // generic
      elementGraph.removeUnnecessaryPipes(); // groups must be added before removing pipes
      elementGraph.resolveFields();

      elementGraph = flow.updateSchemes( elementGraph );

      // m/r specific
      handleAdjacentTaps( elementGraph );

      FlowStepGraph flowStepGraph = new HadoopStepGraph( flowDef.getName(), elementGraph );

      flow.initialize( elementGraph, flowStepGraph );

      return flow;
      }
    catch( Exception exception )
      {
      throw handleExceptionDuringPlanning( exception, elementGraph );
      }
    }

  private void handleWarnEquivalentPaths( ElementGraph elementGraph )
    {
    List<CoGroup> coGroups = elementGraph.findAllCoGroups();

    for( CoGroup coGroup : coGroups )
      {
      List<GraphPath<FlowElement, Scope>> graphPaths = elementGraph.getAllShortestPathsTo( coGroup );

      List<List<FlowElement>> paths = ElementGraphs.asPathList( graphPaths );

      if( !areEquivalentPaths( elementGraph, paths ) )
        continue;

      LOG.warn( "found equivalent paths from: {} to: {}", paths.get( 0 ).get( 1 ), coGroup );

      // in order to remove dupe paths, we need to verify there isn't any branching
      }
    }

  private boolean areEquivalentPaths( ElementGraph elementGraph, List<List<FlowElement>> paths )
    {
    int length = sameLength( paths );

    if( length == -1 )
      return false;

    Set<FlowElement> elements = new TreeSet<FlowElement>( new EquivalenceComparator( elementGraph ) );

    for( int i = 0; i < length; i++ )
      {
      elements.clear();

      for( List<FlowElement> path : paths )
        elements.add( path.get( i ) );

      if( elements.size() != 1 )
        return false;
      }

    return true;
    }

  private class EquivalenceComparator implements Comparator<FlowElement>
    {
    private final ElementGraph elementGraph;

    public EquivalenceComparator( ElementGraph elementGraph )
      {
      this.elementGraph = elementGraph;
      }

    @Override
    public int compare( FlowElement lhs, FlowElement rhs )
      {
      boolean areEquivalent = lhs.isEquivalentTo( rhs );
      boolean sameIncoming = elementGraph.inDegreeOf( lhs ) == elementGraph.inDegreeOf( rhs );
      boolean sameOutgoing = elementGraph.outDegreeOf( lhs ) == elementGraph.outDegreeOf( rhs );

      if( areEquivalent && sameIncoming && sameOutgoing )
        return 0;

      return System.identityHashCode( lhs ) - System.identityHashCode( rhs );
      }
    }

  private int sameLength( List<List<FlowElement>> paths )
    {
    int lastSize = paths.get( 0 ).size();

    for( int i = 1; i < paths.size(); i++ )
      {
      if( paths.get( i ).size() != lastSize )
        return -1;
      }

    return lastSize;
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
        int maxPaths = elementGraph.getMaxNumPathsBetweenElementAndGroupingMergeJoin( flowElement );
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

  /**
   * will collapse adjacent and equivalent taps.
   * equivalence is based on the tap adjacent taps using the same filesystem
   * and the sink being symmetrical, and having the same fields as the temp tap.
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
        if( !successorTap.getScheme().isSymmetrical() )
          continue;

        URI tempURIScheme = getDefaultURIScheme( tap ); // temp uses default fs
        URI successorURIScheme = getURIScheme( successorTap );

        if( !tempURIScheme.equals( successorURIScheme ) )
          continue;

        // safe, both are symmetrical
        // should be called after fields are resolved
        if( !tap.getSourceFields().equals( successorTap.getSourceFields() ) )
          continue;

        elementGraph.replaceElementWith( tap, successor );

        return false;
        }
      }

    return true;
    }

  private URI getDefaultURIScheme( Tap tap )
    {
    return ( (Hfs) tap ).getDefaultFileSystemURIScheme( jobConf );
    }

  private URI getURIScheme( Tap tap )
    {
    return ( (Hfs) tap ).getURIScheme( jobConf );
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
      Set<Tap> taps = new HashSet<Tap>();

      // iterate each shortest path to current group finding each tap sourcing the merge/join
      for( GraphPath<FlowElement, Scope> path : elementGraph.getAllShortestPathsTo( group ) )
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

        // handle case where there is a split on a pipe between the tap and group
        for( GraphPath<FlowElement, Scope> path : getAllShortestPathsBetween( elementGraph, tap, group ) )
          {
          List<FlowElement> flowElements = Graphs.getPathVertexList( path ); // shortest path tap -> group
          Collections.reverse( flowElements ); // group -> tap

          FlowElement flowElement = flowElements.get( 1 );

          if( flowElement instanceof TempHfs )
            continue;

          LOG.warn( "inserting step to normalize incompatible sources: {}", tap );

          insertTempTapAfter( elementGraph, (Pipe) flowElement );

          return false;
          }
        }
      }

    return normalizeGroups.isEmpty();
    }

  @Override
  protected Tap makeTempTap( String prefix, String name )
    {
    // must give Taps unique names
    return new TempHfs( jobConf, Util.makePath( prefix, name ), intermediateSchemeClass, prefix == null );
    }

  private Class getSchemeClass( Tap tap )
    {
    if( tap instanceof TempHfs )
      return ( (TempHfs) tap ).getSchemeClass();
    else
      return tap.getScheme().getClass();
    }
  }
