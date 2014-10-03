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

package cascading.flow.hadoop.planner.rule.scopeexpression;

import java.net.URI;

import cascading.flow.FlowElement;
import cascading.flow.hadoop.planner.HadoopPlanner;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.expression.ScopeExpression;
import cascading.tap.hadoop.Hfs;

/**
 *
 */
public class EquivalentTapsScopeExpression extends ScopeExpression
  {
  @Override
  public boolean applies( PlannerContext plannerContext, ElementGraph elementGraph, Scope scope )
    {
    FlowElement edgeSource = elementGraph.getEdgeSource( scope );
    FlowElement edgeTarget = elementGraph.getEdgeTarget( scope );

    if( !( edgeSource instanceof Hfs ) || !( edgeTarget instanceof Hfs ) )
      throw new IllegalStateException( "non Hfs Taps matched" );

    Hfs predecessor = (Hfs) edgeSource;
    Hfs successor = (Hfs) edgeTarget;

    // does this scheme source what it sinks
    if( !successor.getScheme().isSymmetrical() )
      return false;

    HadoopPlanner flowPlanner = (HadoopPlanner) plannerContext.getFlowPlanner();

    URI tempURIScheme = flowPlanner.getDefaultURIScheme( predecessor ); // temp uses default fs
    URI successorURIScheme = flowPlanner.getURIScheme( successor );

    if( !tempURIScheme.equals( successorURIScheme ) )
      return false;

    // safe, both are symmetrical
    // should be called after fields are resolved
    if( !predecessor.getSourceFields().equals( successor.getSourceFields() ) )
      return true;

    return true;
    }
  }
