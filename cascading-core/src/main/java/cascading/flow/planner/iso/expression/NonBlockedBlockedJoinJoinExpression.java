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

import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.tap.Tap;

import static cascading.flow.planner.iso.expression.OrElementExpression.or;

/**
 *
 */
public class NonBlockedBlockedJoinJoinExpression extends ExpressionGraph
  {
  public NonBlockedBlockedJoinJoinExpression()
    {
    super( SearchOrder.ReverseDepth );

    ElementExpression source = or( ElementCapture.Primary, new FlowElementExpression( Tap.class ), new FlowElementExpression( Group.class ) );
    ElementExpression blocking = or( new FlowElementExpression( HashJoin.class ), new FlowElementExpression( Group.class ) );
    ElementExpression sink = new FlowElementExpression( ElementCapture.Secondary, HashJoin.class );

    this.arc(
      source,
      PathScopeExpression.ANY,
      blocking
    );

    this.arc(
      blocking,
      PathScopeExpression.ANY,
      sink
    );

    this.arc(
      source,
      PathScopeExpression.ANY,
      sink
    );

    }
  }
