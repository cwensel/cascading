/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.flow.tez.planner.rule.expression;

import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.PathScopeExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.rule.RuleExpression;
import cascading.pipe.Boundary;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;

import static cascading.flow.planner.iso.expression.AndElementExpression.and;
import static cascading.flow.planner.iso.expression.NotElementExpression.not;

/**
 *
 */
public class BalanceSplitToStreamedHashJoinExpression extends RuleExpression
  {
  private static final FlowElementExpression SHARED_SPLIT = new FlowElementExpression( Pipe.class, TypeExpression.Topo.Split );
  public static final FlowElementExpression SHARED_HASHJOIN = new FlowElementExpression( HashJoin.class );

  public BalanceSplitToStreamedHashJoinExpression()
    {
    super(
      new ExpressionGraph(
        and(
          ElementCapture.Primary,
          not( and( new FlowElementExpression( Pipe.class, TypeExpression.Topo.Split ), not( new FlowElementExpression( Boundary.class ) ) ) ),
          not( new FlowElementExpression( HashJoin.class ) ) ) ),

      new ExpressionGraph()
        .arcs( SHARED_SPLIT, SHARED_HASHJOIN )
        .arcs( SHARED_SPLIT, SHARED_HASHJOIN ),

      new ExpressionGraph()
        .arc(
          and( ElementCapture.Secondary, new FlowElementExpression( Pipe.class ), not( new FlowElementExpression( Boundary.class ) ) ),
          PathScopeExpression.NON_BLOCKING,
          new FlowElementExpression( ElementCapture.Primary, HashJoin.class )
        )
    );
    }
  }
