/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.PathScopeExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.expressiongraph.SyncPipeExpressionGraph;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

/**
 *
 */
public class BalanceSameSourceStreamedAccumulatedExpression extends RuleExpression
  {
  private static final FlowElementExpression SHARED_TAP = new FlowElementExpression( Tap.class, TypeExpression.Topo.SplitOnly );
  public static final FlowElementExpression SHARED_HASHJOIN_STREAMED = new FlowElementExpression( HashJoin.class );
  public static final FlowElementExpression SHARED_HASHJOIN_ACCUMULATED = new FlowElementExpression( HashJoin.class );

  public BalanceSameSourceStreamedAccumulatedExpression()
    {
    super(
      new SyncPipeExpressionGraph(),

      new ExpressionGraph()
        .arc( SHARED_TAP, PathScopeExpression.NON_BLOCKING, SHARED_HASHJOIN_STREAMED )
        .arc( SHARED_TAP, PathScopeExpression.BLOCKING, SHARED_HASHJOIN_ACCUMULATED )
        .arc( SHARED_HASHJOIN_STREAMED, PathScopeExpression.NON_BLOCKING, SHARED_HASHJOIN_ACCUMULATED ),

      new ExpressionGraph()
        .arc(
          new FlowElementExpression( ElementCapture.Primary, Pipe.class ),
          PathScopeExpression.NON_BLOCKING,
          new FlowElementExpression( ElementCapture.Secondary, HashJoin.class )
        )
    );
    }
  }
