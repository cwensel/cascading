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

package cascading.flow.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowElement;
import cascading.operation.AssertionLevel;
import cascading.operation.DebugLevel;
import cascading.pipe.Checkpoint;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.OperatorException;
import cascading.pipe.Pipe;
import cascading.pipe.Splice;
import cascading.pipe.SubAssembly;
import cascading.property.PropertyUtil;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.util.Util;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.planner.ElementGraphs.*;
import static java.util.Arrays.asList;

/** Class FlowPlanner is the base class for all planner implementations. */
public abstract class FlowPlanner
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( FlowPlanner.class );

  /** Field properties */
  protected Map<Object, Object> properties;

  protected String checkpointRootPath = null;

  /** Field assertionLevel */
  protected AssertionLevel assertionLevel;
  /** Field debugLevel */
  protected DebugLevel debugLevel;

  /**
   * Method getAssertionLevel returns the configured target planner {@link cascading.operation.AssertionLevel}.
   *
   * @param properties of type Map<Object, Object>
   * @return AssertionLevel the configured AssertionLevel
   */
  static AssertionLevel getAssertionLevel( Map<Object, Object> properties )
    {
    String assertionLevel = PropertyUtil.getProperty( properties, "cascading.flowconnector.assertionlevel", AssertionLevel.STRICT.name() );

    return AssertionLevel.valueOf( assertionLevel );
    }

  /**
   * Method getDebugLevel returns the configured target planner {@link cascading.operation.DebugLevel}.
   *
   * @param properties of type Map<Object, Object>
   * @return DebugLevel the configured DebugLevel
   */
  static DebugLevel getDebugLevel( Map<Object, Object> properties )
    {
    String debugLevel = PropertyUtil.getProperty( properties, "cascading.flowconnector.debuglevel", DebugLevel.DEFAULT.name() );

    return DebugLevel.valueOf( debugLevel );
    }

  public abstract PlatformInfo getPlatformInfo();

  public void initialize( FlowConnector flowConnector, Map<Object, Object> properties )
    {
    this.properties = properties;
    this.assertionLevel = getAssertionLevel( properties );
    this.debugLevel = getDebugLevel( properties );
    }

  /**
   * Method buildFlow renders the actual Flow instance.
   *
   * @param flowDef@return Flow
   */
  public abstract Flow buildFlow( FlowDef flowDef );

  /** Must be called to determine if all elements of the base pipe assembly are available */
  protected void verifyAssembly( FlowDef flowDef )
    {
    verifySourceNotSinks( flowDef.getSources(), flowDef.getSinks() );

    verifyTaps( flowDef.getSources(), true, true );
    verifyTaps( flowDef.getSinks(), false, true );
    verifyTaps( flowDef.getTraps(), false, false );

    // are both sources and sinks
    verifyTaps( flowDef.getCheckpoints(), true, false );
    verifyTaps( flowDef.getCheckpoints(), false, false );

    verifyPipeAssemblyEndPoints( flowDef );
    verifyTraps( flowDef );
    verifyCheckpoints( flowDef );
    }

  protected ElementGraph createElementGraph( FlowDef flowDef )
    {
    Pipe[] pipes = flowDef.getTailsArray();
    Map<String, Tap> sources = flowDef.getSourcesCopy();
    Map<String, Tap> sinks = flowDef.getSinksCopy();
    Map<String, Tap> traps = flowDef.getTrapsCopy();
    Map<String, Tap> checkpoints = flowDef.getCheckpointsCopy();

    AssertionLevel assertionLevel = flowDef.getAssertionLevel() == null ? this.assertionLevel : flowDef.getAssertionLevel();
    DebugLevel debugLevel = flowDef.getDebugLevel() == null ? this.debugLevel : flowDef.getDebugLevel();
    checkpointRootPath = makeCheckpointRootPath( flowDef );

    return new ElementGraph( getPlatformInfo(), pipes, sources, sinks, traps, checkpoints, checkpointRootPath != null, assertionLevel, debugLevel );
    }

  private String makeCheckpointRootPath( FlowDef flowDef )
    {
    String flowName = flowDef.getName();
    String runID = flowDef.getRunID();

    if( runID == null )
      return null;

    if( runID != null && flowName == null )
      throw new PlannerException( "flow name is required when providing a run id" );

    return flowName + "/" + runID;
    }


  protected void verifySourceNotSinks( Map<String, Tap> sources, Map<String, Tap> sinks )
    {
    Collection<Tap> sourcesSet = sources.values();

    for( Tap tap : sinks.values() )
      {
      if( sourcesSet.contains( tap ) )
        throw new PlannerException( "tap may not be used as both source and sink in the same Flow: " + tap );
      }
    }

  /**
   * Method verifyTaps ...
   *
   * @param taps          of type Map<String, Tap>
   * @param areSources    of type boolean
   * @param mayNotBeEmpty of type boolean
   */
  protected void verifyTaps( Map<String, Tap> taps, boolean areSources, boolean mayNotBeEmpty )
    {
    if( mayNotBeEmpty && taps.isEmpty() )
      throw new PlannerException( ( areSources ? "source" : "sink" ) + " taps are required" );

    for( String tapName : taps.keySet() )
      {
      if( areSources && !taps.get( tapName ).isSource() )
        throw new PlannerException( "tap named: '" + tapName + "', cannot be used as a source: " + taps.get( tapName ) );
      else if( !areSources && !taps.get( tapName ).isSink() )
        throw new PlannerException( "tap named: '" + tapName + "', cannot be used as a sink: " + taps.get( tapName ) );
      }
    }

  /**
   * Method verifyEndPoints verifies
   * <p/>
   * there aren't dupe names in heads or tails.
   * all the sink and source tap names match up with tail and head pipes
   */
  // todo: force dupe names to throw exceptions
  protected void verifyPipeAssemblyEndPoints( FlowDef flowDef )
    {
    Set<String> tapNames = new HashSet<String>();

    tapNames.addAll( flowDef.getSources().keySet() );
    tapNames.addAll( flowDef.getSinks().keySet() );

    // handle tails
    Set<Pipe> tails = new HashSet<Pipe>();
    Set<String> tailNames = new HashSet<String>();

    for( Pipe pipe : flowDef.getTails() )
      {
      if( pipe instanceof SubAssembly )
        {
        for( Pipe tail : ( (SubAssembly) pipe ).getTails() )
          {
          String tailName = tail.getName();

          if( !tapNames.contains( tailName ) )
            throw new PlannerException( tail, "pipe name not found in either sink or source map: '" + tailName + "'" );

          if( tailNames.contains( tailName ) && !tails.contains( tail ) )
            LOG.warn( "duplicate tail name found: '{}'", tailName );
//            throw new PlannerException( pipe, "duplicate tail name found: " + tailName );

          tailNames.add( tailName );
          tails.add( tail );
          }
        }
      else
        {
        String tailName = pipe.getName();

        if( !tapNames.contains( tailName ) )
          throw new PlannerException( pipe, "pipe name not found in either sink or source map: '" + tailName + "'" );

        if( tailNames.contains( tailName ) && !tails.contains( pipe ) )
          LOG.warn( "duplicate tail name found: '{}'", tailName );
//            throw new PlannerException( pipe, "duplicate tail name found: " + tailName );

        tailNames.add( tailName );
        tails.add( pipe );
        }
      }

//    Set<String> allTailNames = new HashSet<String>( tailNames );
    tailNames.removeAll( flowDef.getSinks().keySet() );
    Set<String> remainingSinks = new HashSet<String>( flowDef.getSinks().keySet() );
    remainingSinks.removeAll( tailNames );

    if( tailNames.size() != 0 )
      throw new PlannerException( "not all tail pipes bound to sink taps, remaining tail pipe names: [" + Util.join( Util.quote( tailNames, "'" ), ", " ) + "], remaining sink tap names: [" + Util.join( Util.quote( remainingSinks, "'" ), ", " ) + "]" );

    // unlike heads, pipes can input to another pipe and simultaneously be a sink
    // so there is no way to know all the intentional tails, so they aren't listed below in the exception
    remainingSinks = new HashSet<String>( flowDef.getSinks().keySet() );
    remainingSinks.removeAll( asList( Pipe.names( flowDef.getTailsArray() ) ) );

    if( remainingSinks.size() != 0 )
      throw new PlannerException( "not all sink taps bound to tail pipes, remaining sink tap names: [" + Util.join( Util.quote( remainingSinks, "'" ), ", " ) + "]" );

    // handle heads
    Set<Pipe> heads = new HashSet<Pipe>();
    Set<String> headNames = new HashSet<String>();

    for( Pipe pipe : flowDef.getTails() )
      {
      for( Pipe head : pipe.getHeads() )
        {
        String headName = head.getName();

        if( !tapNames.contains( headName ) )
          throw new PlannerException( head, "pipe name not found in either sink or source map: '" + headName + "'" );

        if( headNames.contains( headName ) && !heads.contains( head ) )
          LOG.warn( "duplicate head name found, not an error but heads should have unique names: '{}'", headName );
//          throw new PlannerException( pipe, "duplicate head name found: " + headName );

        headNames.add( headName );
        heads.add( head );
        }
      }

    Set<String> allHeadNames = new HashSet<String>( headNames );
    headNames.removeAll( flowDef.getSources().keySet() );
    Set<String> remainingSources = new HashSet<String>( flowDef.getSources().keySet() );
    remainingSources.removeAll( headNames );

    if( headNames.size() != 0 )
      throw new PlannerException( "not all head pipes bound to source taps, remaining head pipe names: [" + Util.join( Util.quote( headNames, "'" ), ", " ) + "], remaining source tap names: [" + Util.join( Util.quote( remainingSources, "'" ), ", " ) + "]" );

    remainingSources = new HashSet<String>( flowDef.getSources().keySet() );
    remainingSources.removeAll( allHeadNames );

    if( remainingSources.size() != 0 )
      throw new PlannerException( "not all source taps bound to head pipes, remaining source tap names: [" + Util.join( Util.quote( remainingSources, "'" ), ", " ) + "], remaining head pipe names: [" + Util.join( Util.quote( headNames, "'" ), ", " ) + "]" );

    }

  protected void verifyTraps( FlowDef flowDef )
    {
    verifyNotSourcesSinks( flowDef.getTraps(), flowDef.getSources(), flowDef.getSinks(), "trap" );

    Set<String> names = new HashSet<String>( asList( Pipe.names( flowDef.getTailsArray() ) ) );

    for( String name : flowDef.getTraps().keySet() )
      {
      if( !names.contains( name ) )
        throw new PlannerException( "trap name not found in assembly: '" + name + "'" );
      }
    }

  protected void verifyCheckpoints( FlowDef flowDef )
    {
    verifyNotSourcesSinks( flowDef.getCheckpoints(), flowDef.getSources(), flowDef.getSinks(), "checkpoint" );

    for( Tap checkpointTap : flowDef.getCheckpoints().values() )
      {
      Scheme scheme = checkpointTap.getScheme();

      if( scheme.getSourceFields().equals( Fields.UNKNOWN ) && scheme.getSinkFields().equals( Fields.ALL ) )
        continue;

      throw new PlannerException( "checkpoint tap scheme must be undeclared, source fields must be UNKNOWN, and sink fields ALL, got: " + scheme.toString() );
      }

    Set<String> names = new HashSet<String>( asList( Pipe.names( flowDef.getTailsArray() ) ) );

    for( String name : flowDef.getCheckpoints().keySet() )
      {
      if( !names.contains( name ) )
        throw new PlannerException( "checkpoint name not found in assembly: '" + name + "'" );

      Set<Pipe> pipes = new HashSet<Pipe>( asList( Pipe.named( name, flowDef.getTailsArray() ) ) );

      int count = 0;

      for( Pipe pipe : pipes )
        {
        if( pipe instanceof Checkpoint )
          count++;
        }

      if( count == 0 )
        throw new PlannerException( "no checkpoint with name found in assembly: '" + name + "'" );

      if( count > 1 )
        throw new PlannerException( "more than one checkpoint with name found in assembly: '" + name + "'" );
      }
    }

  private void verifyNotSourcesSinks( Map<String, Tap> taps, Map<String, Tap> sources, Map<String, Tap> sinks, String role )
    {
    Collection<Tap> sourceTaps = sources.values();
    Collection<Tap> sinkTaps = sinks.values();

    for( Tap tap : taps.values() )
      {
      if( sourceTaps.contains( tap ) )
        throw new PlannerException( "tap may not be used as both a " + role + " and a source in the same Flow: " + tap );

      if( sinkTaps.contains( tap ) )
        throw new PlannerException( "tap may not be used as both a " + role + " and a sink in the same Flow: " + tap );
      }
    }

  /**
   * Verifies that there are not only GroupAssertions following any given Group instance. This will adversely
   * affect the stream entering any subsequent Tap of Each instances.
   */
  protected void failOnLoneGroupAssertion( ElementGraph elementGraph )
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

          if( every.getPlannerLevel() != null )
            assertions++;
          }

        if( everies != 0 && everies == assertions )
          throw new PlannerException( "group assertions must be accompanied by aggregator operations" );
        }
      }
    }

  protected void failOnMissingGroup( ElementGraph elementGraph )
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
          if( flowElement instanceof Each || flowElement instanceof Checkpoint )
            throw new PlannerException( (Pipe) flowElement, "Every may only be preceded by another Every or a Group pipe, found: " + flowElement );

          if( flowElement instanceof Every )
            continue;

          if( flowElement instanceof Group )
            break;
          }
        }
      }
    }

  protected void failOnMisusedBuffer( ElementGraph elementGraph )
    {
    List<Every> everies = elementGraph.findAllEveries();

    // walk Every instances after Group
    for( Every every : everies )
      {
      for( GraphPath<FlowElement, Scope> path : elementGraph.getAllShortestPathsTo( every ) )
        {
        List<FlowElement> flowElements = Graphs.getPathVertexList( path ); // last element is every
        Collections.reverse( flowElements ); // first element is every

        Every last = null;
        boolean foundBuffer = false;
        int foundEveries = -1;

        for( FlowElement flowElement : flowElements )
          {
          if( flowElement instanceof Each )
            throw new PlannerException( (Pipe) flowElement, "Every may only be preceded by another Every or a GroupBy or CoGroup pipe, found: " + flowElement );

          if( flowElement instanceof Every )
            {
            foundEveries++;

            boolean isBuffer = ( (Every) flowElement ).isBuffer();

            if( foundEveries != 0 && ( isBuffer || foundBuffer ) )
              throw new PlannerException( (Pipe) flowElement, "Only one Every with a Buffer may follow a GroupBy or CoGroup pipe, no other Every instances are allowed immediately before or after, found: " + flowElement + " before: " + last );

            if( !foundBuffer )
              foundBuffer = isBuffer;

            last = (Every) flowElement;
            }

          if( flowElement instanceof Group )
            break;
          }
        }
      }
    }

  protected void failOnGroupEverySplit( ElementGraph elementGraph )
    {
    List<Group> groups = new ArrayList<Group>();

    elementGraph.findAllOfType( 1, 2, Group.class, groups );

    for( Group group : groups )
      {
      Set<FlowElement> children = elementGraph.getAllChildrenNotExactlyType( group, Pipe.class );

      for( FlowElement flowElement : children )
        {
        if( flowElement instanceof Every )
          throw new PlannerException( (Every) flowElement, "Every instances may not split after a GroupBy or CoGroup pipe, found: " + flowElement + " after: " + group );
        }
      }
    }

  protected PlannerException handleExceptionDuringPlanning( Exception exception, ElementGraph elementGraph )
    {
    if( exception instanceof PlannerException )
      {
      ( (PlannerException) exception ).elementGraph = elementGraph;

      return (PlannerException) exception;
      }
    else if( exception instanceof ElementGraphException )
      {
      Throwable cause = exception.getCause();

      if( cause == null )
        cause = exception;

      // captures pipegraph for debugging
      // forward message in case cause or trace is lost
      String message = String.format( "could not build flow from assembly: [%s]", cause.getMessage() );

      if( cause instanceof OperatorException )
        return new PlannerException( message, cause, elementGraph );

      if( cause instanceof TapException )
        return new PlannerException( message, cause, elementGraph );

      return new PlannerException( ( (ElementGraphException) exception ).getPipe(), message, cause, elementGraph );
      }
    else
      {
      // captures pipegraph for debugging
      // forward message in case cause or trace is lost
      String message = String.format( "could not build flow from assembly: [%s]", exception.getMessage() );
      return new PlannerException( message, exception, elementGraph );
      }
    }

  protected void handleNonSafeOperations( ElementGraph elementGraph )
    {
    // if there was a graph change, iterate paths again.
    while( !internalNonSafeOperations( elementGraph ) )
      ;
    }

  private boolean internalNonSafeOperations( ElementGraph elementGraph )
    {
    Set<Pipe> tapInsertions = new HashSet<Pipe>();

    List<Pipe> splits = elementGraph.findAllPipeSplits();

    // if any predecessor is unsafe, insert temp
    for( Pipe split : splits )
      {
      List<GraphPath<FlowElement, Scope>> paths = elementGraph.getAllShortestPathsTo( split );

      for( GraphPath<FlowElement, Scope> path : paths )
        {
        List<FlowElement> elements = Graphs.getPathVertexList( path );
        Collections.reverse( elements );

        for( FlowElement element : elements )
          {
          if( !( element instanceof Each ) && element.getClass() != Pipe.class )
            break;

          if( element.getClass() == Pipe.class )
            continue;

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
   * Method insertTapAfter ...
   *
   * @param graph of type PipeGraph
   * @param pipe  of type Pipe
   */
  protected void insertTempTapAfter( ElementGraph graph, Pipe pipe )
    {
    LOG.debug( "inserting tap after: {}", pipe );

    Tap checkpointTap = graph.getCheckpointsMap().get( pipe.getName() );

    if( checkpointTap != null )
      LOG.info( "found checkpoint: {}, using tap: {}", pipe.getName(), checkpointTap );

    if( checkpointTap == null )
      {
      // only restart from a checkpoint pipe or checkpoint tap below
      if( pipe instanceof Checkpoint )
        checkpointTap = makeTempTap( checkpointRootPath, pipe.getName() );
      else
        checkpointTap = makeTempTap( pipe.getName() );
      }

    graph.insertFlowElementAfter( pipe, checkpointTap );
    }

  protected Tap makeTempTap( String name )
    {
    return makeTempTap( null, name );
    }

  protected abstract Tap makeTempTap( String prefix, String name );

  /**
   * Inserts a temporary Tap between logical MR jobs.
   * <p/>
   * Since all joins are at groups or splices, depth first search is safe
   * <p/>
   * todo: refactor so that rules are applied to path segments bounded by taps
   * todo: this would allow balancing of operations within paths instead of pushing
   * todo: all operations up. may allow for consolidation of rules
   *
   * @param elementGraph of type PipeGraph
   */
  protected void handleJobPartitioning( ElementGraph elementGraph )
    {
    // if there was a graph change, iterate paths again. prevents many temp taps from being inserted in front of a group
    while( !internalJobPartitioning( elementGraph ) )
      ;
    }

  private boolean internalJobPartitioning( ElementGraph elementGraph )
    {
    for( GraphPath<FlowElement, Scope> path : elementGraph.getAllShortestPathsBetweenExtents() )
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
          {
          foundGroup = true;
          }
        else if( flowElement instanceof Splice && foundGroup ) // add tap between groups, push joins/merge map side
          {
          tapInsertions.add( (Pipe) flowElements.get( i - 1 ) );

          if( !( flowElement instanceof Group ) )
            foundGroup = false;
          }
        else if( flowElement instanceof Checkpoint ) // add tap after checkpoint
          {
          if( flowElements.get( i + 1 ) instanceof Tap ) // don't keep inserting
            continue;

          tapInsertions.add( (Pipe) flowElement );
          foundGroup = false;
          }
        else if( flowElement instanceof Tap )
          {
          foundGroup = false;
          }
        }

      for( Pipe pipe : tapInsertions )
        insertTempTapAfter( elementGraph, pipe );

      if( !tapInsertions.isEmpty() )
        return false;
      }

    return true;
    }

  /**
   * Prevent leftmost sources from sourcing a downstream join on the rightmost side intra-task by inserting a
   * temp tap between the left-sourced join and right-sourced join.
   *
   * @param elementGraph
   */
  protected void handleJoins( ElementGraph elementGraph )
    {
    while( !internalJoins( elementGraph ) )
      ;
    }

  private boolean internalJoins( ElementGraph elementGraph )
    {
    List<GraphPath<FlowElement, Scope>> paths = elementGraph.getAllShortestPathsBetweenExtents();

    // large to small
    Collections.reverse( paths );

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      List<FlowElement> flowElements = Graphs.getPathVertexList( path );
      List<Pipe> tapInsertions = new ArrayList<Pipe>();
      List<HashJoin> joins = new ArrayList<HashJoin>();
      List<Merge> merges = new ArrayList<Merge>();

      FlowElement lastSourceElement = null;

      for( int i = 0; i < flowElements.size(); i++ )
        {
        FlowElement flowElement = flowElements.get( i );

        if( flowElement instanceof Merge )
          {
          merges.add( (Merge) flowElement );
          }
        else if( flowElement instanceof HashJoin )
          {
          HashJoin join = (HashJoin) flowElement;

          Map<Integer, Integer> pathCounts = countOrderedDirectPathsBetween( elementGraph, lastSourceElement, join, true );

          // is this path streamed
          int pathPosition = pathPositionInto( path, join );
          boolean thisPathIsStreamed = pathPosition == 0;

          boolean isAccumulatedAndStreamed = isBothAccumulatedAndStreamedPath( pathCounts ); // has streamed and accumulated paths
          int pathCount = countPaths( pathCounts );

          int priorJoins = countTypesBetween( elementGraph, lastSourceElement, join, HashJoin.class );

          if( priorJoins == 0 )
            {
            // if same source is leading into the hashjoin, insert tap on the accumulated side
            if( pathCount == 2 && isAccumulatedAndStreamed && !thisPathIsStreamed )
              {
              tapInsertions.add( (Pipe) flowElements.get( flowElements.indexOf( join ) - 1 ) );
              break;
              }

            // if more than one path into streamed and accumulated branches, insert tap on streamed side
            if( pathCount > 2 && isAccumulatedAndStreamed && thisPathIsStreamed )
              {
              tapInsertions.add( (Pipe) flowElements.get( flowElements.indexOf( join ) - 1 ) );
              break;
              }
            }

          if( !merges.isEmpty() )
            {
            // if a Merge is prior to a HashJoin, and its an accumulated path, force Merge results to disk
            int joinPos = flowElements.indexOf( join );
            int mergePos = nearest( flowElements, joinPos, merges );

            if( mergePos != -1 && joinPos > mergePos )
              {
              // if all paths are accumulated and streamed, insert
              // else if just if this path is accumulated
              if( ( isAccumulatedAndStreamed && thisPathIsStreamed ) || !thisPathIsStreamed )
                {
                tapInsertions.add( (Pipe) flowElements.get( flowElements.indexOf( join ) - 1 ) );
                break;
                }
              }
            }

          joins.add( (HashJoin) flowElement );
          }
        else if( flowElement instanceof Tap || flowElement instanceof Group )
          {
          for( int j = 0; j < joins.size(); j++ )
            {
            HashJoin join = joins.get( j );

            int pathPosition = pathPositionInto( path, join );
            boolean thisPathIsStreamed = pathPosition == 0;

            Map<Integer, Integer> pathCounts = countOrderedDirectPathsBetween( elementGraph, lastSourceElement, join, true );

            boolean isAccumulatedAndStreamed = isBothAccumulatedAndStreamedPath( pathCounts ); // has streamed and accumulated paths
            int pathCount = countPaths( pathCounts );

            if( pathCount >= 2 && isAccumulatedAndStreamed && thisPathIsStreamed )
              {
              tapInsertions.add( (Pipe) flowElements.get( flowElements.indexOf( join ) - 1 ) );
              break;
              }

            if( thisPathIsStreamed )
              continue;

            if( j == 0 ) // is accumulated on first join
              break;

            // prevent a streamed path from being accumulated by injecting a tap before the
            // current HashJoin
            tapInsertions.add( (Pipe) flowElements.get( flowElements.indexOf( join ) - 1 ) );
            break;
            }

          if( !tapInsertions.isEmpty() )
            break;

          lastSourceElement = flowElement;
          merges.clear();
          joins.clear();
          }
        }

      for( Pipe pipe : tapInsertions )
        insertTempTapAfter( elementGraph, pipe );

      if( !tapInsertions.isEmpty() )
        return false;
      }

    return true;
    }

  private int nearest( List<FlowElement> flowElements, int index, List<Merge> merges )
    {
    List<Merge> reversed = new ArrayList<Merge>( merges );
    Collections.reverse( reversed );

    for( Merge merge : reversed )
      {
      int pos = flowElements.indexOf( merge );
      if( pos < index )
        return pos;
      }

    return -1;
    }
  }
