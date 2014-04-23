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

package cascading.flow.planner.rule.transformer;

import cascading.flow.planner.iso.transformer.ContractedGraphTransformer;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.RuleTransformer;

/**
 *
 */
public class RuleContractedTransformer extends RuleTransformer
  {
  public RuleContractedTransformer( PlanPhase phase, RuleExpression ruleExpression )
    {
    super( phase, ruleExpression );

    if( subGraphTransformer != null )
      flowGraphTransformer = new ContractedGraphTransformer( subGraphTransformer, ruleExpression.getMatchExpression() );
    else if( contractedTransformer != null )
      flowGraphTransformer = new ContractedGraphTransformer( contractedTransformer, ruleExpression.getMatchExpression() );
    else
      flowGraphTransformer = new ContractedGraphTransformer( ruleExpression.getMatchExpression() );
    }
  }
