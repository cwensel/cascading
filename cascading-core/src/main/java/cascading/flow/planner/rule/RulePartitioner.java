/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

import java.util.Collection;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.subgraph.GraphPartitioner;
import cascading.flow.planner.iso.subgraph.Partitions;
import cascading.flow.planner.rule.util.LogLevel;

import static cascading.flow.planner.rule.util.RuleLogUtil.enableLogging;
import static cascading.flow.planner.rule.util.RuleLogUtil.restoreLogging;

/**
 * The RulePartitioner class is responsible for partitioning an element graph into smaller sub-graphs.
 * <p>
 * It may also re-partition a given graph, in place replacing it with its children, if any.
 */
public abstract class RulePartitioner implements Rule
  {
  public abstract Enum[] getAnnotationExcludes();

  public enum PartitionSource
    {
      /**
       * Partition the parent into children.
       */
      PartitionParent,

      /**
       * Partition a given child into more children, removing the original child.
       */
      PartitionCurrent
    }

  protected LogLevel logLevel;
  protected PlanPhase phase;
  protected PartitionSource partitionSource = PartitionSource.PartitionParent;
  protected GraphPartitioner graphPartitioner;

  public RulePartitioner( PlanPhase phase, PartitionSource partitionSource, GraphPartitioner graphPartitioner )
    {
    this( null, phase, partitionSource, graphPartitioner );
    }

  public RulePartitioner( LogLevel logLevel, PlanPhase phase, PartitionSource partitionSource, GraphPartitioner graphPartitioner )
    {
    this.logLevel = logLevel;
    this.phase = phase;
    this.partitionSource = partitionSource;
    this.graphPartitioner = graphPartitioner;
    }

  public RulePartitioner()
    {
    }

  @Override
  public PlanPhase getRulePhase()
    {
    return phase;
    }

  public PartitionSource getPartitionSource()
    {
    return partitionSource;
    }

  protected GraphPartitioner getGraphPartitioner()
    {
    return graphPartitioner;
    }

  public Partitions partition( PlannerContext plannerContext, ElementGraph elementGraph )
    {
    return partition( plannerContext, elementGraph, null );
    }

  public Partitions partition( PlannerContext plannerContext, ElementGraph elementGraph, Collection<FlowElement> excludes )
    {
    String[] logLevels = enableLogging( logLevel );

    try
      {
      return performPartition( plannerContext, elementGraph, excludes );
      }
    finally
      {
      restoreLogging( logLevels );
      }
    }

  public Partitions performPartition( PlannerContext plannerContext, ElementGraph elementGraph, Collection<FlowElement> excludes )
    {
    Partitions partitions = getGraphPartitioner().partition( plannerContext, elementGraph, excludes );

    if( partitions != null )
      partitions.setRulePartitioner( this );

    return partitions;
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
