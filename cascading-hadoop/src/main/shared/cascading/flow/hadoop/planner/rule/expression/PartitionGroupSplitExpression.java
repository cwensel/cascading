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

package cascading.flow.hadoop.planner.rule.expression;

import cascading.flow.planner.iso.expression.ElementExpression;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.SyncPipeExpressionGraph;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.rule.RuleExpression;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import static cascading.flow.planner.iso.expression.OrElementExpression.or;

/**
 *
 */
public class PartitionGroupSplitExpression extends RuleExpression
  {
  public static final FlowElementExpression SHARED_GROUP = new FlowElementExpression( Group.class );

  public PartitionGroupSplitExpression()
    {
    super(
      new SyncPipeExpressionGraph(),

      // in order to capture out degree in sub-graph, we need to capture at least two successors
      new ExpressionGraph()
        .arcs( SHARED_GROUP, or( new FlowElementExpression( HashJoin.class ), new FlowElementExpression( Group.class ), new FlowElementExpression( Tap.class ) ) )
        .arcs( SHARED_GROUP, or( new FlowElementExpression( HashJoin.class ), new FlowElementExpression( Group.class ), new FlowElementExpression( Tap.class ) ) ),

      // sub-graph to match has out degree captured above
      new ExpressionGraph()
        .arcs(
          new FlowElementExpression( ElementExpression.Capture.Primary, Pipe.class, TypeExpression.Topo.Split )
        )
    );
    }
  }
