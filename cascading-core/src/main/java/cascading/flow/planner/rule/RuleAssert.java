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

package cascading.flow.planner.rule;

import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.assertion.GraphAssert;
import cascading.flow.planner.iso.transformer.ContractedTransformer;
import cascading.flow.planner.iso.transformer.SubGraphTransformer;
import cascading.flow.planner.iso.transformer.Transformed;

/**
 * The RuleAssert class is responsible for asserting the structure of a element graph.
 */
public class RuleAssert extends GraphAssert<ElementGraph> implements Rule
  {
  private final PlanPhase phase;
  private final RuleExpression ruleExpression;
  private ContractedTransformer contractedTransformer;
  private SubGraphTransformer subGraphTransformer;

  public RuleAssert( PlanPhase phase, RuleExpression ruleExpression, String message )
    {
    this( phase, ruleExpression, message, null );
    }

  public RuleAssert( PlanPhase phase, RuleExpression ruleExpression, String message, AssertionType assertionType )
    {
    super( ruleExpression.getMatchExpression(), message, assertionType );
    this.phase = phase;
    this.ruleExpression = ruleExpression;

    if( ruleExpression.getContractionExpression() != null )
      contractedTransformer = new ContractedTransformer( ruleExpression.getContractionExpression() );
    else
      contractedTransformer = null;

    if( ruleExpression.getContractedMatchExpression() != null )
      {
      if( contractedTransformer == null )
        throw new IllegalArgumentException( "must have contracted expression if given contracted match expression" );

      subGraphTransformer = new SubGraphTransformer( contractedTransformer, ruleExpression.getContractedMatchExpression() );
      }
    else
      {
      subGraphTransformer = null;
      }
    }

  @Override
  public PlanPhase getRulePhase()
    {
    return phase;
    }

  @Override
  public String getRuleName()
    {
    return getClass().getSimpleName().replaceAll( "^(.*)[A-Z][a-z]*Rule$", "$1" );
    }

  @Override
  protected Transformed<ElementGraph> transform( PlannerContext plannerContext, ElementGraph graph )
    {
    Transformed transformed = null;

    if( contractedTransformer != null )
      transformed = contractedTransformer.transform( plannerContext, graph );
    else if( subGraphTransformer != null )
      transformed = subGraphTransformer.transform( plannerContext, graph );

    return transformed;
    }

  @Override
  public String toString()
    {
    return getRuleName();
    }
  }
