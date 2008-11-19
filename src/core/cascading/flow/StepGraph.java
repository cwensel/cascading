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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

import cascading.pipe.EndPipe;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tap.TempHfs;
import org.apache.log4j.Logger;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

/** Class StepGraph ... */
public class StepGraph extends SimpleDirectedGraph<FlowStep, Integer>
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( StepGraph.class );

  /** Constructor StepGraph creates a new StepGraph instance. */
  StepGraph()
    {
    super( Integer.class );
    }

  /**
   * Constructor StepGraph creates a new StepGraph instance.
   *
   * @param elementGraph of type ElementGraph
   * @param traps        of type Map<String, Tap>
   */
  StepGraph( ElementGraph elementGraph, Map<String, Tap> traps )
    {
    this();

    makeStepGraph( elementGraph, traps );

    validateGraph( traps );
    }

  private void validateGraph( Map<String, Tap> traps )
    {
    verifyTrapsAreUnique( traps );

    traps = new HashMap<String, Tap>( traps ); // make copy

    TopologicalOrderIterator<FlowStep, Integer> iterator = getTopologicalIterator();

    while( iterator.hasNext() )
      {
      FlowStep step = iterator.next();

      verifyTraps( traps, step.mapperTraps );
      verifyTraps( traps, step.reducerTraps );
      }
    }

  private void verifyTrapsAreUnique( Map<String, Tap> traps )
    {
    for( Tap tap : traps.values() )
      {
      if( Collections.frequency( traps.values(), tap ) != 1 )
        throw new PlannerException( "traps must be unique, cannot be reused on different branches: " + tap );
      }
    }

  private void verifyTraps( Map<String, Tap> traps, Map<String, Tap> map )
    {
    for( String name : map.keySet() )
      {
      if( !traps.containsKey( name ) )
        throw new PlannerException( "traps may not cross Map and Reduce boundaries: " + name );
      else
        traps.remove( name );
      }
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

  /**
   * Creates the map reduce step graph.
   *
   * @param elementGraph
   * @param traps
   */
  private void makeStepGraph( ElementGraph elementGraph, Map<String, Tap> traps )
    {
    SimpleDirectedGraph<Tap, Integer> tapGraph = elementGraph.makeTapGraph();

    Map<String, FlowStep> steps = new LinkedHashMap<String, FlowStep>();
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

        addVertex( step );

        if( steps.containsKey( source.toString() ) )
          addEdge( steps.get( source.toString() ), step, count++ );

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

          boolean onMapSide = true;

          for( Scope scope : scopes )
            {
            FlowElement rhs = elementGraph.getEdgeTarget( scope );

            step.graph.addVertex( rhs );
            step.graph.addEdge( lhs, rhs, scope );

            if( rhs instanceof Group )
              {
              step.group = (Group) rhs;
              onMapSide = false;
              }
            else if( rhs instanceof Pipe ) // add relevant traps to step
              {
              String name = ( (Pipe) rhs ).getName();

              if( traps.containsKey( name ) )
                {
                if( onMapSide )
                  step.mapperTraps.put( name, traps.get( name ) );
                else
                  step.reducerTraps.put( name, traps.get( name ) );
                }
              }

            lhs = rhs;
            }
          }
        }
      }
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

  public TopologicalOrderIterator<FlowStep, Integer> getTopologicalIterator()
    {
    return new TopologicalOrderIterator<FlowStep, Integer>( this );
    }

  }
