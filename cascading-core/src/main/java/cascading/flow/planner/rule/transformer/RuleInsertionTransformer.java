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

package cascading.flow.planner.rule.transformer;

import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.transformer.InsertionGraphTransformer;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.RuleTransformer;
import cascading.flow.planner.rule.util.LogLevel;

/**
 *
 */
public class RuleInsertionTransformer extends RuleTransformer
  {
  public RuleInsertionTransformer( PlanPhase phase, RuleExpression ruleExpression, String factoryName )
    {
    this( null, phase, ruleExpression, null, factoryName );
    }

  public RuleInsertionTransformer( PlanPhase phase, RuleExpression ruleExpression, String factoryName, InsertionGraphTransformer.Insertion insertion )
    {
    this( null, phase, ruleExpression, null, factoryName, insertion );
    }

  public RuleInsertionTransformer( PlanPhase phase, RuleExpression ruleExpression, ElementCapture capture, String factoryName )
    {
    this( null, phase, ruleExpression, capture, factoryName, InsertionGraphTransformer.Insertion.After );
    }

  public RuleInsertionTransformer( PlanPhase phase, RuleExpression ruleExpression, ElementCapture capture, String factoryName, InsertionGraphTransformer.Insertion insertion )
    {
    this( null, phase, ruleExpression, capture, factoryName, insertion );
    }

  public RuleInsertionTransformer( LogLevel logLevel, PlanPhase phase, RuleExpression ruleExpression, String factoryName )
    {
    this( logLevel, phase, ruleExpression, null, factoryName );
    }

  public RuleInsertionTransformer( LogLevel logLevel, PlanPhase phase, RuleExpression ruleExpression, String factoryName, InsertionGraphTransformer.Insertion insertion )
    {
    this( logLevel, phase, ruleExpression, null, factoryName, insertion );
    }

  public RuleInsertionTransformer( LogLevel logLevel, PlanPhase phase, RuleExpression ruleExpression, ElementCapture capture, String factoryName )
    {
    this( logLevel, phase, ruleExpression, capture, factoryName, InsertionGraphTransformer.Insertion.After );
    }

  public RuleInsertionTransformer( LogLevel logLevel, PlanPhase phase, RuleExpression ruleExpression, ElementCapture capture, String factoryName, InsertionGraphTransformer.Insertion insertion )
    {
    super( logLevel, phase, ruleExpression );

    if( subGraphTransformer != null )
      graphTransformer = new InsertionGraphTransformer( subGraphTransformer, ruleExpression.getMatchExpression(), capture, factoryName, insertion );
    else if( contractedTransformer != null )
      graphTransformer = new InsertionGraphTransformer( contractedTransformer, ruleExpression.getMatchExpression(), capture, factoryName, insertion );
    else
      graphTransformer = new InsertionGraphTransformer( ruleExpression.getMatchExpression(), capture, factoryName, insertion );
    }
  }
