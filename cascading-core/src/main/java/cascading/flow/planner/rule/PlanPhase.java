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

    // Flow step graph rules
    PartitionSteps( Stage.Steps ),

    // Final state
    Complete( Stage.Terminal );

  public static PlanPhase prior( PlanPhase phase )
    {
    if( phase == Start )
      return null;

    return values()[ phase.ordinal() - 1 ];
    }

  private enum Stage
    {
      Terminal, Elements, Steps
    }

  Stage stage;

  PlanPhase( Stage stage )
    {
    this.stage = stage;
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
  }
