/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
import java.util.Arrays;
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
import cascading.flow.Scope;
import cascading.operation.AssertionLevel;
import cascading.operation.DebugLevel;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.OperatorException;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.util.Util;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class FlowPlanner is the base class for all planner implementations. */
public abstract class FlowPlanner
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( FlowPlanner.class );

  /** Field properties */
  protected Map<Object, Object> properties;

  /** Field assertionLevel */
  protected AssertionLevel assertionLevel;
  /** Field debugLevel */
  protected DebugLevel debugLevel;

  public void initialize( FlowConnector flowConnector, Map<Object, Object> properties )
    {
    this.properties = properties;
    this.assertionLevel = FlowConnector.getAssertionLevel( properties );
    this.debugLevel = FlowConnector.getDebugLevel( properties );
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

    verifyPipeAssemblyEndPoints( flowDef );
    verifyTraps( flowDef );
    }

  protected ElementGraph createElementGraph( FlowDef flowDef )
    {
    Pipe[] pipes = flowDef.getTailsArray();
    Map<String, Tap> sources = flowDef.getSourcesCopy();
    Map<String, Tap> sinks = flowDef.getSinksCopy();
    Map<String, Tap> traps = flowDef.getTrapsCopy();
    AssertionLevel assertionLevel = flowDef.getAssertionLevel() == null ? this.assertionLevel : flowDef.getAssertionLevel();
    DebugLevel debugLevel = flowDef.getDebugLevel() == null ? this.debugLevel : flowDef.getDebugLevel();

    return new ElementGraph( pipes, sources, sinks, traps, assertionLevel, debugLevel );
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
    remainingSinks.removeAll( Arrays.asList( Pipe.names( flowDef.getTailsArray() ) ) );

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
    verifyTrapsNotSourcesSinks( flowDef.getTraps(), flowDef.getSources(), flowDef.getSinks() );

    Set<String> names = new HashSet<String>();

    Collections.addAll( names, Pipe.names( flowDef.getTailsArray() ) );

    for( String name : flowDef.getTraps().keySet() )
      {
      if( !names.contains( name ) )
        throw new PlannerException( "trap name not found in assembly: '" + name + "'" );
      }
    }

  private void verifyTrapsNotSourcesSinks( Map<String, Tap> traps, Map<String, Tap> sources, Map<String, Tap> sinks )
    {
    Collection<Tap> sourceTaps = sources.values();
    Collection<Tap> sinkTaps = sinks.values();

    for( Tap tap : traps.values() )
      {
      if( sourceTaps.contains( tap ) )
        throw new PlannerException( "tap may not be used as both a trap and a source in the same Flow: " + tap );

      if( sinkTaps.contains( tap ) )
        throw new PlannerException( "tap may not be used as both a trap and a sink in the same Flow: " + tap );
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
          if( flowElement instanceof Each )
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

    graph.insertFlowElementAfter( pipe, makeTempTap( pipe.getName() ) );
    }

  /**
   * Method makeTemp ...
   *
   * @param name
   * @return Tap
   */
  protected abstract Tap makeTempTap( String name );

  /**
   * Inserts a temporary Tap between logical MR jobs.
   * <p/>
   * Since all joins are at groups, depth first search is safe
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
  }
