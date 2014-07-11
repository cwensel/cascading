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

package cascading.flow.planner.rule.expressiongraph;

import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.iso.expression.AndElementExpression;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.pipe.Boundary;
import cascading.pipe.Checkpoint;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.tap.Tap;

import static cascading.flow.planner.iso.expression.NotElementExpression.not;

/**
 *
 */
public class SyncPipeExpressionGraph extends ExpressionGraph
  {
  public SyncPipeExpressionGraph()
    {
    super( new AndElementExpression(
      ElementCapture.Primary,
      not( new FlowElementExpression( Extent.class ) ),
      not( new FlowElementExpression( Tap.class ) ),
      not( new FlowElementExpression( Boundary.class ) ),
      not( new FlowElementExpression( Checkpoint.class ) ),
      not( new FlowElementExpression( Group.class ) ),
      not( new FlowElementExpression( HashJoin.class ) ) ) );
    }
  }
