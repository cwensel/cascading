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
    Start( Stage.Terminal ),

    // Flow element graph rules
    PrePartitionElements( Stage.Elements ),
    PartitionElements( Stage.Elements ),
    PreResolveElements( Stage.Elements ),
    ResolveElements( Stage.Elements ),
    PostResolveElements( Stage.Elements ),

    // Flow step sub-graph partition rules
    PartitionSteps( Stage.Steps ),

    // Flow node sub-graph partition rules
    PartitionNodes( Stage.Nodes ),
    PipelineNodes( Stage.Nodes ),
    PostPipelineNodes( Stage.Nodes ),

    // Final state
    Complete( Stage.Terminal );

  public static PlanPhase prior( PlanPhase phase )
    {
    if( phase == Start )
      return null;

    return values()[ phase.ordinal() - 1 ];
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
      Elements,

      /**
       * Applies to generating sub-graphs of flow steps where a step is a submitted unit of cluster work.
       */
      Steps,

      /**
       * Applies to generation sub-graphs of flow step nodes where is a node is a unit of processing with a step.
       * <p/>
       * In MapReduce its a Map or Reduce node (where Map and Reduce are Kinds). In Tez its a Processor.
       */
      Nodes
    }

  Stage stage;

  PlanPhase( Stage stage )
    {
    this.stage = stage;
    }

  public Stage getStage()
    {
    return stage;
    }

  public boolean isTerminal()
    {
    return stage == Stage.Terminal;
    }

  public boolean isElements()
    {
    return stage == Stage.Elements;
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
