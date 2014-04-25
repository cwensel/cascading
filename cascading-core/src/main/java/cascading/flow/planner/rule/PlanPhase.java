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

/**
 *
 */
public enum PlanPhase
  {
    // Initial state
    Start( Role.System, Stage.Terminal, Mode.None ),

    // Flow element graph rules
    PreBalanceAssembly( Role.User, Stage.Assembly, Mode.Mutate ),
    BalanceAssembly( Role.User, Stage.Assembly, Mode.Mutate ),
    PostBalanceAssembly( Role.User, Stage.Assembly, Mode.Mutate ),

    PreResolveAssembly( Role.User, Stage.Assembly, Mode.Mutate ),
    ResolveAssembly( Role.System, Stage.Assembly, Mode.Mutate ),
    PostResolveAssembly( Role.User, Stage.Assembly, Mode.Mutate ),

    // Flow step sub-graph partition rules
    PartitionSteps( Role.User, Stage.Steps, Mode.Partition ),

    // Flow node sub-graph partition rules
    PartitionNodes( Role.User, Stage.Nodes, Mode.Partition ),

    // Flow node pipeline sub-graph partition rules
    PartitionPipelines( Role.User, Stage.Pipelines, Mode.Partition ),
    PostPipelines( Role.User, Stage.Pipelines, Mode.Mutate ),

    // Final state
    Complete( Role.System, Stage.Terminal, Mode.None );

  public static PlanPhase prior( PlanPhase phase )
    {
    if( phase == Start )
      return null;

    return values()[ phase.ordinal() - 1 ];
    }

  public enum Role
    {
      System, User
    }

  public enum Stage
    {
      /**
       * Start or Complete
       */
      Terminal,

      /**
       * Applies to FlowElement assertions or transformations.
       */
      Assembly,

      /**
       * Applies to generating sub-graphs of flow steps where a step is a submitted unit of cluster work.
       */
      Steps,

      /**
       * Applies to generation sub-graphs of flow step nodes where is a node is a unit of processing with a step.
       * <p/>
       * In MapReduce its a Map or Reduce node (where Map and Reduce are Kinds). In Tez its a Processor.
       */
      Nodes,

      /**
       * Applies to sub-graphs with flow nodes having a single streamed input, and multiple accumulated inputs.
       */
      Pipelines
    }

  public enum Mode
    {
      None,
      Mutate,
      Partition
    }

  Role role;
  Stage stage;
  Mode mode;

  PlanPhase( Role role, Stage stage, Mode mode )
    {
    this.role = role;
    this.stage = stage;
    this.mode = mode;
    }

  public Role getRole()
    {
    return role;
    }

  public boolean isSystem()
    {
    return role == Role.System;
    }

  public boolean isUser()
    {
    return role == Role.User;
    }

  public Stage getStage()
    {
    return stage;
    }

  public boolean isTerminal()
    {
    return stage == Stage.Terminal;
    }

  public boolean isAssembly()
    {
    return stage == Stage.Assembly;
    }

  public boolean isSteps()
    {
    return stage == Stage.Steps;
    }

  public boolean isNodes()
    {
    return stage == Stage.Nodes;
    }
  }
