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

public class RuleExpression
  {
  protected final ExpressionGraph contractionExpression;
  protected final ExpressionGraph contractedMatchExpression;
  protected final ExpressionGraph matchExpression;

  public RuleExpression( ExpressionGraph matchExpression )
    {
    this.contractionExpression = null;
    this.contractedMatchExpression = null;
    this.matchExpression = matchExpression;

    verify();
    }

  public RuleExpression( ExpressionGraph contractionExpression, ExpressionGraph matchExpression )
    {
    this.contractionExpression = contractionExpression;
    this.contractedMatchExpression = null;
    this.matchExpression = matchExpression;

    verify();
    }

  public RuleExpression( ExpressionGraph contractionExpression, ExpressionGraph contractedMatchExpression, ExpressionGraph matchExpression )
    {
    this.contractionExpression = contractionExpression;
    this.contractedMatchExpression = contractedMatchExpression;
    this.matchExpression = matchExpression;

    verify();
    }

  private void verify()
    {
    // test for
    // constraction removes
    // contractedMatch
    // matchExpression inserts
    }

  public String getExpressionName()
    {
    return getClass().getSimpleName().replaceAll( "^(.*)[]A-Z][a-z]*Rule$", "$1" );
    }

  public ExpressionGraph getContractionExpression()
    {
    return contractionExpression;
    }

  public ExpressionGraph getContractedMatchExpression()
    {
    return contractedMatchExpression;
    }

  public ExpressionGraph getMatchExpression()
    {
    return matchExpression;
    }
  }
