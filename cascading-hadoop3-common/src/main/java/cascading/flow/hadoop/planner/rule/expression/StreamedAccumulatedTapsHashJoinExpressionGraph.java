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
import cascading.flow.planner.iso.expression.PathScopeExpression;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.tap.Tap;

import static cascading.flow.planner.iso.expression.OrElementExpression.or;

/**
 *
 */
public class StreamedAccumulatedTapsHashJoinExpressionGraph extends ExpressionGraph
  {
  public StreamedAccumulatedTapsHashJoinExpressionGraph()
    {
    super( SearchOrder.Depth );

    FlowElementExpression split = new FlowElementExpression( Tap.class );

    ElementExpression join = new FlowElementExpression( HashJoin.class );

    ElementExpression merge = new FlowElementExpression( Merge.class );

    ElementExpression sink = or(
      ElementCapture.Secondary,
      new FlowElementExpression( Tap.class ),
      new FlowElementExpression( Group.class )
    );

    this.arc(
      new FlowElementExpression( ElementCapture.Primary, Tap.class ),
      PathScopeExpression.ALL_NON_BLOCKING,
      join
    );

    this.arc(
      split,
      PathScopeExpression.ALL_BLOCKING, // if ANY_BLOCKING DualStreamedAccumulatedMergePipelineAssert will fire
      join
    );

    this.arc(
      join,
      PathScopeExpression.ANY,
      merge
    );

    this.arc(
      split,
      PathScopeExpression.NO_CAPTURE,
      merge
    );

    this.arc(
      merge,
      PathScopeExpression.ANY,
      sink
    );
    }
  }
