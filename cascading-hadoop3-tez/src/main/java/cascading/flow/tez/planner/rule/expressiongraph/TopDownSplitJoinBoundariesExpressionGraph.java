/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.tez.planner.rule.expressiongraph;

import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.PathScopeExpression;
import cascading.flow.planner.iso.expression.TypeExpression.Topo;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.flow.planner.rule.elementexpression.BoundariesElementExpression;
import cascading.tap.Tap;

import static cascading.flow.planner.iso.expression.AndElementExpression.and;
import static cascading.flow.planner.iso.expression.NotElementExpression.not;

/**
 *
 */
public class TopDownSplitJoinBoundariesExpressionGraph extends ExpressionGraph
  {
  public TopDownSplitJoinBoundariesExpressionGraph()
    {
    super( SearchOrder.Topological );

    this.arc(
      new BoundariesElementExpression( Topo.Split ),

      PathScopeExpression.ANY,

      // don't capture a contracted graph having a tap split to a splice tap (two taps with two edges)
      // this interferes if there is a logical split and merge
      and(
        ElementCapture.Primary,
        new BoundariesElementExpression( Topo.Splice ),
        not( new FlowElementExpression( Tap.class ) )
      )
    );
    }
  }
