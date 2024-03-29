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

package cascading.flow.planner.iso.expression;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.pipe.Operator;

/**
 *
 */
public class NonSafeOperationExpression extends TypeExpression<Operator>
  {
  public NonSafeOperationExpression( ElementCapture capture )
    {
    super( capture, Operator.class );
    }

  public NonSafeOperationExpression( Topo topo )
    {
    super( Operator.class, topo );
    }

  public NonSafeOperationExpression()
    {
    super( Operator.class );
    }

  @Override
  public boolean applies( PlannerContext flowPlanner, ElementGraph elementGraph, FlowElement flowElement )
    {
    boolean applies = super.applies( flowPlanner, elementGraph, flowElement );

    return applies && !( (Operator) flowElement ).getOperation().isSafe();
    }
  }
