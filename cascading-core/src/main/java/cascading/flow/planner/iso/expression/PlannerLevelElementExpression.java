/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.operation.PlannedOperation;
import cascading.operation.PlannerLevel;
import cascading.pipe.Operator;

/**
 *
 */
public class PlannerLevelElementExpression extends ElementExpression
  {
  private final Class<? extends PlannerLevel> plannerLevelClass;

  protected PlannerLevelElementExpression( ElementCapture capture, Class<? extends PlannerLevel> plannerLevelClass )
    {
    super( capture );
    this.plannerLevelClass = plannerLevelClass;
    }

  @Override
  public boolean applies( PlannerContext plannerContext, ElementGraph elementGraph, FlowElement flowElement )
    {
    if( !( flowElement instanceof Operator ) )
      return false;

    Operator operator = (Operator) flowElement;

    if( !operator.hasPlannerLevel() )
      return false;

    PlannerLevel plannerLevel = plannerContext.getPlannerLevelFor( plannerLevelClass );

    if( plannerLevel == null )
      return false;

    if( !( (PlannedOperation) operator.getOperation() ).supportsPlannerLevel( plannerLevel ) )
      return false;

    return operator.getPlannerLevel().isStricterThan( plannerLevel );
    }
  }
