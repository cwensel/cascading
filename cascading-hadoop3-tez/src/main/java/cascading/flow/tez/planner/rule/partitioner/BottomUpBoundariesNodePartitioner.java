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

package cascading.flow.tez.planner.rule.partitioner;

import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.expressiongraph.NoGroupJoinMergeBoundaryTapExpressionGraph;
import cascading.flow.planner.rule.partitioner.ExpressionRulePartitioner;
import cascading.flow.stream.graph.IORole;
import cascading.flow.tez.planner.rule.expressiongraph.BottomUpNoSplitConsecutiveBoundariesExpressionGraph;

import static cascading.flow.planner.rule.PlanPhase.PartitionNodes;

/**
 *
 */
public class BottomUpBoundariesNodePartitioner extends ExpressionRulePartitioner
  {
  public BottomUpBoundariesNodePartitioner()
    {
    super(
      PartitionNodes,

      new RuleExpression(
        new NoGroupJoinMergeBoundaryTapExpressionGraph(),
        new BottomUpNoSplitConsecutiveBoundariesExpressionGraph()
      ),

      new ElementAnnotation( ElementCapture.Primary, IORole.sink )
    );
    }
  }
