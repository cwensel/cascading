/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner.rule;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.flow.planner.ScopedElement;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.graph.FlowElementGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ScopeResolver
  {
  private static final Logger LOG = LoggerFactory.getLogger( ScopeResolver.class );

  public static void resolveFields( FlowElementGraph flowElementGraph )
    {
    if( flowElementGraph.isResolved() )
      throw new IllegalStateException( "element graph already resolved" );

    Iterator<FlowElement> iterator = ElementGraphs.getTopologicalIterator( flowElementGraph );

    while( iterator.hasNext() )
      resolveFields( flowElementGraph, iterator.next() );

    flowElementGraph.setResolved( true );
    }

  private static void resolveFields( FlowElementGraph flowElementGraph, FlowElement source )
    {
    if( source instanceof Extent )
      return;

    Set<Scope> incomingScopes = flowElementGraph.incomingEdgesOf( source );
    Set<Scope> outgoingScopes = flowElementGraph.outgoingEdgesOf( source );

    List<FlowElement> flowElements = flowElementGraph.successorListOf( source );

    if( flowElements.size() == 0 )
      throw new IllegalStateException( "unable to find next elements in pipeline from: " + source.toString() );

    if( !( source instanceof ScopedElement ) )
      throw new IllegalStateException( "flow element is not a scoped element: " + source.toString() );

    Scope outgoingScope = ( (ScopedElement) source ).outgoingScopeFor( incomingScopes );

    if( LOG.isDebugEnabled() && outgoingScope != null )
      {
      LOG.debug( "for modifier: " + source );
      if( outgoingScope.getArgumentsSelector() != null )
        LOG.debug( "setting outgoing arguments: " + outgoingScope.getArgumentsSelector() );
      if( outgoingScope.getOperationDeclaredFields() != null )
        LOG.debug( "setting outgoing declared: " + outgoingScope.getOperationDeclaredFields() );
      if( outgoingScope.getKeySelectors() != null )
        LOG.debug( "setting outgoing group: " + outgoingScope.getKeySelectors() );
      if( outgoingScope.getOutValuesSelector() != null )
        LOG.debug( "setting outgoing values: " + outgoingScope.getOutValuesSelector() );
      }

    for( Scope scope : outgoingScopes )
      scope.copyFields( outgoingScope );
    }
  }
