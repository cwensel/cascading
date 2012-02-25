/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.Scope;
import cascading.flow.planner.ElementGraph;
import cascading.flow.planner.FlowStep;
import cascading.flow.planner.FlowStepGraph;
import cascading.flow.planner.PlannerException;
import cascading.pipe.Group;
import cascading.pipe.Join;
import cascading.pipe.Pipe;
import cascading.pipe.Splice;
import cascading.tap.Tap;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.planner.ElementGraphs.countOrderedDirectPathsBetween;
import static cascading.flow.planner.ElementGraphs.getAllShortestPathsBetween;

/**
 *
 */
public class HadoopStepGraph extends FlowStepGraph
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HadoopStepGraph.class );

  public HadoopStepGraph()
    {
    }

  public HadoopStepGraph( String flowName, ElementGraph elementGraph )
    {
    super( flowName, elementGraph );
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
   */
  protected void makeStepGraph( String flowName, ElementGraph elementGraph )
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

        HadoopFlowStep step = (HadoopFlowStep) getCreateFlowStep( steps, sink.toString(), numJobs );

        addVertex( step );

        if( steps.containsKey( source.toString() ) )
          addEdge( steps.get( source.toString() ), step, count++ );

        populateStep( elementGraph, source, sink, step );
        }
      }
    }

  private void populateStep( ElementGraph elementGraph, Tap source, Tap sink, HadoopFlowStep step )
    {
    Map<String, Tap> traps = elementGraph.getTrapMap();

    // support multiple paths from source to sink
    // this allows for self joins on groups, even with different operation stacks between them
    // note we must ignore paths with intermediate taps
    List<GraphPath<FlowElement, Scope>> paths = getAllShortestPathsBetween( elementGraph, source, sink );

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
        else if( rhs instanceof Join )
          {
          if( !onMapSide )
            throw new PlannerException( "joins must not present Reduce side" );

          Map<Integer, Integer> sourcePaths = countOrderedDirectPathsBetween( elementGraph, source, (Splice) rhs );

          if( sourcePaths.containsKey( 0 ) )
            step.addJoinWithLeftMost( (Join) rhs, source );
          else
            step.addJoinWithRightMost( (Join) rhs, source );
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
