/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.planner.iso.expression;

import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;

/**
 *
 */
public class JoinEdgesSameSourceScopeExpression extends ScopeExpression
  {
  public static final JoinEdgesSameSourceScopeExpression ALL_SAME_SOURCE = new JoinEdgesSameSourceScopeExpression();

  public JoinEdgesSameSourceScopeExpression()
    {
    }

  @Override
  public boolean applies( PlannerContext plannerContext, ElementGraph elementGraph, Scope scope )
    {
    FlowElement edgeSource = elementGraph.getEdgeSource( scope );
    FlowElement edgeTarget = elementGraph.getEdgeTarget( scope );
    Set<Scope> allEdgesBetween = elementGraph.getAllEdges( edgeSource, edgeTarget );

    if( allEdgesBetween.size() == 1 )
      return false;

    return allEdgesBetween.equals( elementGraph.incomingEdgesOf( edgeTarget ) );
    }
  }
