/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.planner.rule.partitioner;

import java.util.ArrayList;
import java.util.Arrays;

import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.subgraph.GraphPartitioner;
import cascading.flow.planner.iso.subgraph.partitioner.ExpressionGraphPartitioner;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.RulePartitioner;
import cascading.flow.planner.rule.util.LogLevel;

/**
 * Class ExpressionRulePartitioner relies on a {@link cascading.flow.planner.rule.RuleExpression} to identify
 * sub-graphs as partitions.
 */
public class ExpressionRulePartitioner extends RulePartitioner
  {
  Enum[] annotationExcludes = new Enum[ 0 ];

  public ExpressionRulePartitioner( PlanPhase phase, RuleExpression ruleExpression )
    {
    this( null, phase, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression() );
    }

  public ExpressionRulePartitioner( PlanPhase phase, RuleExpression ruleExpression, ElementAnnotation... annotations )
    {
    this( null, phase, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression(), annotations );
    }

  public ExpressionRulePartitioner( PlanPhase phase, RuleExpression ruleExpression, Enum... annotationExcludes )
    {
    this( null, phase, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression() );

    if( annotationExcludes != null )
      this.annotationExcludes = annotationExcludes;
    }

  public ExpressionRulePartitioner( PlanPhase phase, PartitionSource partitionSource, RuleExpression ruleExpression )
    {
    this( null, phase, partitionSource, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression() );
    }

  public ExpressionRulePartitioner( PlanPhase phase, PartitionSource partitionSource, RuleExpression ruleExpression, ElementAnnotation... annotations )
    {
    this( null, phase, partitionSource, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression(), annotations );
    }

  public ExpressionRulePartitioner( PlanPhase phase, PartitionSource partitionSource, RuleExpression ruleExpression, Enum... annotationExcludes )
    {
    this( null, phase, partitionSource, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression() );

    if( annotationExcludes != null )
      this.annotationExcludes = annotationExcludes;
    }

  public ExpressionRulePartitioner( LogLevel logLevel, PlanPhase phase, RuleExpression ruleExpression )
    {
    this( logLevel, phase, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression() );
    }

  public ExpressionRulePartitioner( LogLevel logLevel, PlanPhase phase, RuleExpression ruleExpression, ElementAnnotation... annotations )
    {
    this( logLevel, phase, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression(), annotations );
    }

  public ExpressionRulePartitioner( LogLevel logLevel, PlanPhase phase, RuleExpression ruleExpression, Enum... annotationExcludes )
    {
    this( logLevel, phase, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression() );

    if( annotationExcludes != null )
      this.annotationExcludes = annotationExcludes;
    }

  public ExpressionRulePartitioner( LogLevel logLevel, PlanPhase phase, PartitionSource partitionSource, RuleExpression ruleExpression )
    {
    this( logLevel, phase, partitionSource, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression() );
    }

  public ExpressionRulePartitioner( LogLevel logLevel, PlanPhase phase, PartitionSource partitionSource, RuleExpression ruleExpression, ElementAnnotation... annotations )
    {
    this( logLevel, phase, partitionSource, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression(), annotations );
    }

  public ExpressionRulePartitioner( LogLevel logLevel, PlanPhase phase, PartitionSource partitionSource, RuleExpression ruleExpression, Enum... annotationExcludes )
    {
    this( logLevel, phase, partitionSource, ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression() );

    if( annotationExcludes != null )
      this.annotationExcludes = annotationExcludes;
    }

  protected ExpressionRulePartitioner( PlanPhase phase, GraphPartitioner graphPartitioner )
    {
    this.phase = phase;
    this.graphPartitioner = graphPartitioner;
    }

  protected ExpressionRulePartitioner( LogLevel logLevel, PlanPhase phase, GraphPartitioner graphPartitioner )
    {
    this.logLevel = logLevel;
    this.phase = phase;
    this.graphPartitioner = graphPartitioner;
    }

  private ExpressionRulePartitioner( LogLevel logLevel, PlanPhase phase, ExpressionGraph contractionGraph, ExpressionGraph expressionGraph, ElementAnnotation... annotations )
    {
    this.logLevel = logLevel;
    this.phase = phase;
    this.graphPartitioner = createExpressionGraphPartitioner( contractionGraph, expressionGraph, annotations );
    }

  private ExpressionRulePartitioner( LogLevel logLevel, PlanPhase phase, PartitionSource partitionSource, ExpressionGraph contractionGraph, ExpressionGraph expressionGraph, ElementAnnotation... annotations )
    {
    this.logLevel = logLevel;
    this.phase = phase;
    this.partitionSource = partitionSource;
    this.graphPartitioner = createExpressionGraphPartitioner( contractionGraph, expressionGraph, annotations );
    }

  protected ExpressionRulePartitioner( PlanPhase phase )
    {
    this.phase = phase;
    }

  protected ExpressionRulePartitioner( LogLevel logLevel, PlanPhase phase )
    {
    this.logLevel = logLevel;
    this.phase = phase;
    }

  protected ExpressionRulePartitioner()
    {
    }

  protected ExpressionGraphPartitioner createExpressionGraphPartitioner( ExpressionGraph contractionGraph, ExpressionGraph expressionGraph, ElementAnnotation[] annotations )
    {
    return new ExpressionGraphPartitioner( contractionGraph, expressionGraph, annotations );
    }

  private ExpressionGraphPartitioner getExpressionGraphPartitioner()
    {
    return (ExpressionGraphPartitioner) graphPartitioner;
    }

  protected ExpressionRulePartitioner setPhase( PlanPhase phase )
    {
    this.phase = phase;

    return this;
    }

  public ExpressionRulePartitioner setRuleExpression( RuleExpression ruleExpression )
    {
    this.graphPartitioner = createExpressionGraphPartitioner( ruleExpression.getContractionExpression(), ruleExpression.getMatchExpression(), new ElementAnnotation[ 0 ] );

    return this;
    }

  public ExpressionRulePartitioner addAnnotation( ElementAnnotation annotation )
    {
    ArrayList<ElementAnnotation> elementAnnotations = new ArrayList<>( Arrays.asList( getExpressionGraphPartitioner().getAnnotations() ) );

    elementAnnotations.add( annotation );

    getExpressionGraphPartitioner().setAnnotations( elementAnnotations.toArray( new ElementAnnotation[ elementAnnotations.size() ] ) );

    return this;
    }

  public RulePartitioner setAnnotations( ElementAnnotation... annotations )
    {
    getExpressionGraphPartitioner().setAnnotations( annotations );

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

  @Override
  public Enum[] getAnnotationExcludes()
    {
    return annotationExcludes;
    }
  }
