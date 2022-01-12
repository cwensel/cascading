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
import cascading.flow.planner.iso.expression.NonSafeOperationExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.expressiongraph.NonSafeAndSplitAndSyncPipeExpressionGraph;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

/**
 * Captures a split against a non-safe Operation to prevent the operation from running
 * in parallel mappers
 */
public class BalanceNonSafeSplitExpression extends RuleExpression
  {
  public BalanceNonSafeSplitExpression()
    {
    super(
      new NonSafeAndSplitAndSyncPipeExpressionGraph(),

      new ExpressionGraph()
        .arcs(
          new FlowElementExpression( Tap.class ),
          new NonSafeOperationExpression( TypeExpression.Topo.SplitOnly )
        ),

      new ExpressionGraph()
        .arcs(
          new FlowElementExpression( ElementCapture.Primary, Pipe.class, TypeExpression.Topo.Tail )
        )
    );
    }
  }
