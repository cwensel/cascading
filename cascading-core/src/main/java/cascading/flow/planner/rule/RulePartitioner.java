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

import java.util.ArrayList;
import java.util.Arrays;

import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.subgraph.GraphPartitioner;

/**
 *
 */
public class RulePartitioner extends GraphPartitioner implements Rule
  {
  PlanPhase phase;
  Enum[] annotationExcludes = new Enum[ 0 ];

  public RulePartitioner( PlanPhase phase, RuleExpression ruleExpression )
    {
    this( phase, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression() );
    }

  public RulePartitioner( PlanPhase phase, RuleExpression ruleExpression, ElementAnnotation... annotations )
    {
    this( phase, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression(), annotations );
    }

  public RulePartitioner( PlanPhase phase, RuleExpression ruleExpression, Enum... annotationExcludes )
    {
    this( phase, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression() );

    if( annotationExcludes != null )
      this.annotationExcludes = annotationExcludes;
    }

  protected RulePartitioner( PlanPhase phase, ExpressionGraph contractionGraph, ExpressionGraph expressionGraph, ElementAnnotation... annotations )
    {
    super( contractionGraph, expressionGraph, annotations );
    this.phase = phase;
    }

  protected RulePartitioner( PlanPhase phase, ExpressionGraph expressionGraph )
    {
    super( null, expressionGraph );
    this.phase = phase;
    }

  protected RulePartitioner( PlanPhase phase )
    {
    this.phase = phase;
    }

  public RulePartitioner()
    {
    }

  protected RulePartitioner setPhase( PlanPhase phase )
    {
    this.phase = phase;

    return this;
    }

  @Override
  public PlanPhase getRulePhase()
    {
    return phase;
    }

  public RulePartitioner setRuleExpression( RuleExpression ruleExpression )
    {
    this.contractionGraph = ruleExpression.getContractionExpression();
    this.expressionGraph = ruleExpression.getMatchExpression();

    return this;
    }

  public RulePartitioner addAnnotation( ElementAnnotation annotation )
    {
    ArrayList<ElementAnnotation> elementAnnotations = new ArrayList<>( Arrays.asList( this.annotations ) );

    elementAnnotations.add( annotation );

    this.annotations = elementAnnotations.toArray( new ElementAnnotation[ elementAnnotations.size() ] );

    return this;
    }

  public RulePartitioner setAnnotations( ElementAnnotation... annotations )
    {
    this.annotations = annotations;

    return this;
    }

  public void setAnnotationExcludes( Enum... annotationExcludes )
    {
    if( annotationExcludes != null )
      this.annotationExcludes = annotationExcludes;
    }

  public RulePartitioner addAnnotationExclude( Enum exclude )
    {
    ArrayList<Enum> exclusions = new ArrayList<>( Arrays.asList( this.annotationExcludes ) );

    exclusions.add( exclude );

    this.annotationExcludes = exclusions.toArray( new Enum[ exclusions.size() ] );

    return this;
    }

  public Enum[] getAnnotationExcludes()
    {
    return annotationExcludes;
    }

  @Override
  public String getRuleName()
    {
    return getClass().getSimpleName().replaceAll( "^(.*)[]A-Z][a-z]*Rule$", "$1" );
    }

  @Override
  public String toString()
    {
    return getRuleName();
    }
  }
