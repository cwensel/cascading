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

package cascading.flow.planner.rule;

import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.subgraph.ElementAnnotation;
import cascading.flow.planner.iso.subgraph.GraphPartitioner;

/**
 *
 */
public class RulePartitioner extends GraphPartitioner implements Rule
  {
  PlanPhase phase;

  public RulePartitioner( PlanPhase phase, RuleExpression ruleExpression )
    {
    this( phase, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression() );
    }

  public RulePartitioner( PlanPhase phase, ElementAnnotation annotation, RuleExpression ruleExpression )
    {
    this( phase, annotation, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression() );
    }

  public RulePartitioner( PlanPhase phase, ExpressionGraph contractionGraph, ExpressionGraph expressionGraph )
    {
    this( phase, null, contractionGraph, expressionGraph );
    }

  public RulePartitioner( PlanPhase phase, ElementAnnotation annotation, ExpressionGraph contractionGraph, ExpressionGraph expressionGraph )
    {
    super( annotation, contractionGraph, expressionGraph );
    this.phase = phase;
    }

  public RulePartitioner( PlanPhase phase, ExpressionGraph expressionGraph )
    {
    super( null, expressionGraph );
    this.phase = phase;
    }

  public RulePartitioner( PlanPhase phase )
    {
    this.phase = phase;
    }

  @Override
  public PlanPhase getRulePhase()
    {
    return phase;
    }

  @Override
  public String getRuleName()
    {
    return getClass().getSimpleName().replaceAll( "^(.*)[]A-Z][a-z]*Rule$", "$1" );
    }
  }
