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

package cascading.flow.planner.iso.expression;

import cascading.flow.planner.FlowElementGraph;
import cascading.pipe.Checkpoint;
import cascading.pipe.Group;
import cascading.tap.Tap;

import static cascading.flow.planner.iso.expression.NotElementExpression.not;

/**
 *
 */
public class PartitioningPipeExpressionGraph extends ExpressionGraph
  {
  public PartitioningPipeExpressionGraph()
    {
    super( new AndElementExpression(
      ElementExpression.Capture.Primary,
      not( new FlowElementExpression( FlowElementGraph.Extent.class ) ),
      not( new FlowElementExpression( Tap.class ) ),
      not( new FlowElementExpression( Checkpoint.class ) ),
      not( new FlowElementExpression( Group.class ) ) ) );
    }
  }
