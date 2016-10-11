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

package cascading.flow.tez.planner.rule.expressiongraph;

import cascading.flow.planner.iso.expression.AnnotationExpression;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ElementExpression;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.ScopeExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.flow.planner.rule.elementexpression.BoundariesElementExpression;
import cascading.flow.stream.graph.IORole;
import cascading.pipe.Boundary;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.tap.Tap;

import static cascading.flow.planner.iso.expression.AndElementExpression.and;
import static cascading.flow.planner.iso.expression.NotElementExpression.not;
import static cascading.flow.planner.iso.expression.OrElementExpression.or;

/**
 *
 */
public class BottomUpConsecutiveBoundariesTriangleExpressionGraph extends ExpressionGraph
  {
  public BottomUpConsecutiveBoundariesTriangleExpressionGraph()
    {
    super( SearchOrder.ReverseTopological );

    ElementExpression lhs = or(
      new FlowElementExpression( Boundary.class ),
      new FlowElementExpression( Tap.class ),
      new FlowElementExpression( Group.class, TypeExpression.Topo.LinearOut ),
      new FlowElementExpression( Merge.class, TypeExpression.Topo.LinearOut )
    );

    ElementExpression rhs = or(
      new FlowElementExpression( Boundary.class ),
      new FlowElementExpression( Tap.class ),
      new FlowElementExpression( Group.class, TypeExpression.Topo.LinearOut ),
      new FlowElementExpression( Merge.class, TypeExpression.Topo.LinearOut )
    );

    FlowElementExpression shared = new FlowElementExpression( ElementCapture.Secondary, HashJoin.class );

    ElementExpression tail = or(
      ElementCapture.Primary,
      and(
        new BoundariesElementExpression( TypeExpression.Topo.LinearIn ),
        not( new AnnotationExpression( IORole.sink ) )
      ),
      new BoundariesElementExpression( TypeExpression.Topo.Splice )
    );

    this.arc(
      lhs,

      ScopeExpression.ANY,

      shared
    );

    this.arc(
      rhs,

      ScopeExpression.ANY,

      shared
    );

    this.arc(
      shared,

      ScopeExpression.ANY,

      tail
    );

    this.arc(
      rhs,

      ScopeExpression.NO_CAPTURE, // match, but do not include edge in result graph

      tail
    );
    }
  }
