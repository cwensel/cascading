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

package cascading.flow.planner.rule.expression;

import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ElementExpression;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.expressiongraph.ConsecutiveNoOpPipesExpressionGraph;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import static cascading.flow.planner.iso.expression.OrElementExpression.or;

/**
 * This rule prevents a multi-graph from being formed when all no-op Pipe instances are removed from the assembly.
 * <p>
 * Adding support for multi-graphs in the FlowElementGraph is the better long term solution.
 */
public class NoOpPipeMultiGraphExpression extends RuleExpression
  {
  public static final ElementExpression SPLIT = or(
    new FlowElementExpression( Pipe.class ), new FlowElementExpression( Tap.class )
  );

  public static final ElementExpression JOIN = or(
    new FlowElementExpression( Pipe.class ), new FlowElementExpression( Tap.class )
  );

  public NoOpPipeMultiGraphExpression()
    {
    super(
      new ConsecutiveNoOpPipesExpressionGraph(),
      new ExpressionGraph()
        .arcs(
          SPLIT,
          new FlowElementExpression( ElementCapture.Primary, true, Pipe.class ),
          JOIN
        )
        .arcs(
          SPLIT,
          new FlowElementExpression( ElementCapture.Secondary, true, Pipe.class ),
          JOIN
        )
    );
    }
  }
