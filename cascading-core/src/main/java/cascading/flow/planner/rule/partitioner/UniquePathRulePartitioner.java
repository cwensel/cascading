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

package cascading.flow.planner.rule.partitioner;

import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.subgraph.GraphPartitioner;
import cascading.flow.planner.iso.subgraph.partitioner.ExpressionGraphPartitioner;
import cascading.flow.planner.iso.subgraph.partitioner.UniquePathGraphPartitioner;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.RuleExpression;

/**
 * Class UniquePathRulePartitioner relies on a {@link cascading.flow.planner.rule.RuleExpression} to identify
 * sub-graphs as initial partitions, then will partition the resulting graph into a unique sub-graph for each
 * unique path between the head and tail of the graph.
 * <p/>
 * This partitioner currently requires the matched sub-graph (per the RuleExpression) to have a single head and single
 * tail. All paths will between the matched head and tail.
 *
 * Any remaining elements from the original graph will be included in the final path sub-graph.
 *
 */
public class UniquePathRulePartitioner extends ExpressionRulePartitioner
  {
  public UniquePathRulePartitioner( PlanPhase phase, RuleExpression ruleExpression )
    {
    super( phase, ruleExpression );
    }

  public UniquePathRulePartitioner( PlanPhase phase, RuleExpression ruleExpression, ElementAnnotation... annotations )
    {
    super( phase, ruleExpression, annotations );
    }

  public UniquePathRulePartitioner( PlanPhase phase, RuleExpression ruleExpression, Enum... annotationExcludes )
    {
    super( phase, ruleExpression, annotationExcludes );
    }

  public UniquePathRulePartitioner( PlanPhase phase, PartitionSource partitionSource, RuleExpression ruleExpression )
    {
    super( phase, partitionSource, ruleExpression );
    }

  public UniquePathRulePartitioner( PlanPhase phase, PartitionSource partitionSource, RuleExpression ruleExpression, ElementAnnotation... annotations )
    {
    super( phase, partitionSource, ruleExpression, annotations );
    }

  public UniquePathRulePartitioner( PlanPhase phase, PartitionSource partitionSource, RuleExpression ruleExpression, Enum... annotationExcludes )
    {
    super( phase, partitionSource, ruleExpression, annotationExcludes );
    }

  protected UniquePathRulePartitioner( PlanPhase phase, GraphPartitioner graphPartitioner )
    {
    super( phase, graphPartitioner );
    }

  protected UniquePathRulePartitioner( PlanPhase phase )
    {
    super( phase );
    }

  protected UniquePathRulePartitioner()
    {
    }

  @Override
  protected ExpressionGraphPartitioner createExpressionGraphPartitioner( ExpressionGraph contractionGraph, ExpressionGraph expressionGraph, ElementAnnotation[] annotations )
    {
    // include remainders by default
    return new UniquePathGraphPartitioner( contractionGraph, expressionGraph, true, annotations );
    }
  }
