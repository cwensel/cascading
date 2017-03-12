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

package cascading.flow.hadoop.planner.rule.expression;

import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ElementExpression;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.ScopeExpression;
import cascading.flow.planner.iso.expression.TypeExpression.Topo;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.expressiongraph.SyncPipeExpressionGraph;
import cascading.pipe.Checkpoint;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import static cascading.flow.planner.iso.expression.OrElementExpression.or;

/**
 *
 */
public class BalanceGroupSplitTriangleExpression extends RuleExpression
  {
  public static final FlowElementExpression SHARED_GROUP = new FlowElementExpression( Group.class, Topo.Split );
  public static final ElementExpression SHARED_LHS = or( new FlowElementExpression( HashJoin.class ), new FlowElementExpression( Group.class ), new FlowElementExpression( Tap.class ), new FlowElementExpression( Checkpoint.class ) );
  public static final ElementExpression SHARED_RHS = or( new FlowElementExpression( HashJoin.class ), new FlowElementExpression( Group.class ), new FlowElementExpression( Tap.class ), new FlowElementExpression( Checkpoint.class ) );

  public BalanceGroupSplitTriangleExpression()
    {
    super(
      new SyncPipeExpressionGraph(),

      new ExpressionGraph()
        .arc(
          SHARED_GROUP,
          ScopeExpression.ANY,
          SHARED_LHS
        )

        .arc(
          SHARED_GROUP,
          ScopeExpression.ANY,
          SHARED_RHS
        )

        .arc(
          SHARED_LHS,
          ScopeExpression.ANY,
          SHARED_RHS
        ),

      // sub-graph to match has out degree captured above
      new ExpressionGraph()
        .arcs(
          new FlowElementExpression( ElementCapture.Primary, Pipe.class, Topo.SplitOnly )
        )
    );
    }
  }
