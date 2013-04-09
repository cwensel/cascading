/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner;

import java.util.List;

import cascading.flow.AssemblyPlanner;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.pipe.Pipe;

class AssemblyPlannerContext implements AssemblyPlanner.Context
  {
  private final FlowDef flowDef;
  private final Flow flow;
  private final List<Pipe> tails;

  /**
   * @param flowDef the current FlowDef
   * @param flow    the current Flow
   * @param tails   the current collection of tail pipes
   */
  public AssemblyPlannerContext( FlowDef flowDef, Flow flow, List<Pipe> tails )
    {
    this.flowDef = flowDef;
    this.flow = flow;
    this.tails = tails;
    }

  @Override
  public FlowDef getFlowDef()
    {
    return flowDef;
    }

  @Override
  public Flow getFlow()
    {
    return flow;
    }

  @Override
  public List<Pipe> getTails()
    {
    return tails;
    }
  }
