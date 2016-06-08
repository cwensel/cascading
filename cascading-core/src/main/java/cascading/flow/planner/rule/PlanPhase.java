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

/**
 *
 */
public enum PlanPhase
  {
    // Flow element graph rules
    PreBalanceAssembly( ProcessLevel.Assembly, RuleMode.Mutate, ExecAction.Rule ),
    BalanceAssembly( ProcessLevel.Assembly, RuleMode.Mutate, ExecAction.Rule ),
    PostBalanceAssembly( ProcessLevel.Assembly, RuleMode.Mutate, ExecAction.Rule ),

    PreResolveAssembly( ProcessLevel.Assembly, RuleMode.Mutate, ExecAction.Rule ),
    ResolveAssembly( ProcessLevel.Assembly, RuleMode.Mutate, ExecAction.Resolve ), // force assembly resolution
    PostResolveAssembly( ProcessLevel.Assembly, RuleMode.Mutate, ExecAction.Rule ),

    // Flow step sub-graph partition rules
    PartitionSteps( ProcessLevel.Step, RuleMode.Partition, ExecAction.Rule ),
    PostSteps( ProcessLevel.Step, RuleMode.Mutate, ExecAction.Rule ),

    // Flow node sub-graph partition rules
    PartitionNodes( ProcessLevel.Node, RuleMode.Partition, ExecAction.Rule ),
    PostNodes( ProcessLevel.Node, RuleMode.Mutate, ExecAction.Rule ),

    // Flow node pipeline sub-graph partition rules
    PartitionPipelines( ProcessLevel.Pipeline, RuleMode.Partition, ExecAction.Rule ),
    PostPipelines( ProcessLevel.Pipeline, RuleMode.Mutate, ExecAction.Rule );

  ProcessLevel level;
  ExecAction action;
  RuleMode mode;

  PlanPhase( ProcessLevel level, RuleMode mode, ExecAction action )
    {
    this.level = level;
    this.action = action;
    this.mode = mode;
    }

  public ProcessLevel getLevel()
    {
    return level;
    }

  public ExecAction getAction()
    {
    return action;
    }

  public RuleMode getMode()
    {
    return mode;
    }
  }
