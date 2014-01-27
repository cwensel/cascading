/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.cascade.planner;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import cascading.cascade.CascadeException;
import cascading.flow.BaseFlow;
import cascading.flow.Flow;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class FlowGraph extends SimpleDirectedGraph<Flow, Integer>
  {
  private static final Logger LOG = LoggerFactory.getLogger( FlowGraph.class );

  public FlowGraph( IdentifierGraph identifierGraph )
    {
    super( Integer.class );

    makeGraph( identifierGraph );

    verifyNoCycles();
    }

  public TopologicalOrderIterator<Flow, Integer> getTopologicalIterator()
    {
    return new TopologicalOrderIterator<Flow, Integer>( this, new PriorityQueue<Flow>( 10, new Comparator<Flow>()
    {
    @Override
    public int compare( Flow lhs, Flow rhs )
      {
      return Integer.valueOf( lhs.getSubmitPriority() ).compareTo( rhs.getSubmitPriority() );
      }
    } ) );
    }

  private void verifyNoCycles()
    {
    Set<Flow> flows = new HashSet<Flow>();

    TopologicalOrderIterator<Flow, Integer> topoIterator = new TopologicalOrderIterator<Flow, Integer>( this );

    while( topoIterator.hasNext() )
      flows.add( topoIterator.next() );

    if( flows.size() != vertexSet().size() )
      throw new CascadeException( "there are likely cycles in the set of given flows, topological iterator cannot traverse flows with cycles" );
    }

  private void makeGraph( IdentifierGraph identifierGraph )
    {
    Set<String> identifiers = identifierGraph.vertexSet();

    int count = 0;

    for( String source : identifiers )
      {
      if( LOG.isDebugEnabled() )
        LOG.debug( "handling flow source: {}", source );

      List<String> sinks = Graphs.successorListOf( identifierGraph, source );

      for( String sink : sinks )
        {
        if( LOG.isDebugEnabled() )
          LOG.debug( "handling flow path: {} -> {}", source, sink );

        Flow flow = identifierGraph.getEdge( source, sink ).flow;

        addVertex( flow );

        Set<BaseFlow.FlowHolder> previous = identifierGraph.incomingEdgesOf( source );

        for( BaseFlow.FlowHolder previousFlow : previous )
          {
          addVertex( previousFlow.flow );

          if( getEdge( previousFlow.flow, flow ) != null )
            continue;

          if( !addEdge( previousFlow.flow, flow, count++ ) )
            throw new CascadeException( "unable to add path between: " + previousFlow.flow.getName() + " and: " + flow.getName() );
          }
        }
      }
    }
  }
