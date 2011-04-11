/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.Scope;
import cascading.flow.planner.ElementGraph;
import cascading.flow.planner.FlowStep;
import cascading.flow.planner.StepGraph;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HadoopStepGraph extends StepGraph
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HadoopStepGraph.class );

  public HadoopStepGraph()
    {
    }

  public HadoopStepGraph( String flowName, ElementGraph elementGraph, Map<String, Tap> traps )
    {
    super( flowName, elementGraph, traps );
    }

  protected FlowStep createFlowStep( String stepName, int stepNum )
    {
    return new HadoopFlowStep( stepName, stepNum );
    }

  /**
   * Creates the map reduce step graph.
   *
   * @param flowName
   * @param elementGraph
   * @param traps
   */
  protected void makeStepGraph( String flowName, ElementGraph elementGraph, Map<String, Tap> traps )
    {
    SimpleDirectedGraph<Tap, Integer> tapGraph = elementGraph.makeTapGraph();

    int numJobs = countNumJobs( tapGraph );

    Map<String, FlowStep> steps = new LinkedHashMap<String, FlowStep>();
    TopologicalOrderIterator<Tap, Integer> topoIterator = new TopologicalOrderIterator<Tap, Integer>( tapGraph );
    int count = 0;

    while( topoIterator.hasNext() )
      {
      Tap source = topoIterator.next();

      LOG.debug( "handling source: {}", source );

      List<Tap> sinks = Graphs.successorListOf( tapGraph, source );

      for( Tap sink : sinks )
        {
        LOG.debug( "handling path: {} -> {}", source, sink );

        HadoopFlowStep step = (HadoopFlowStep) getCreateFlowStep( flowName, steps, sink.toString(), numJobs );

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
          String sinkName = scopes.get( scopes.size() - 1 ).getName();

          step.addSource( sourceName, source );
          step.addSink( sinkName, sink );

          FlowElement lhs = source;

          step.getGraph().addVertex( lhs );

          boolean onMapSide = true;

          for( Scope scope : scopes )
            {
            FlowElement rhs = elementGraph.getEdgeTarget( scope );

            step.getGraph().addVertex( rhs );
            step.getGraph().addEdge( lhs, rhs, scope );

            if( rhs instanceof Group )
              {
              step.addGroup( (Group) rhs );
              onMapSide = false;
              }
            else if( rhs instanceof Pipe ) // add relevant traps to step
              {
              String name = ( (Pipe) rhs ).getName();

              // this is legacy, can probably now collapse into one collection safely
              if( traps.containsKey( name ) )
                {
                if( onMapSide )
                  step.getMapperTraps().put( name, traps.get( name ) );
                else
                  step.getReducerTraps().put( name, traps.get( name ) );
                }
              }

            lhs = rhs;
            }
          }
        }
      }
    }

  private int countNumJobs( SimpleDirectedGraph<Tap, Integer> tapGraph )
    {
    Set<Tap> vertices = tapGraph.vertexSet();
    int count = 0;

    for( Tap vertex : vertices )
      {
      if( tapGraph.inDegreeOf( vertex ) != 0 )
        count++;
      }

    return count;
    }
  }
