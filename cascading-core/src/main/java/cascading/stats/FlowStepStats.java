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

package cascading.stats;

import cascading.flow.FlowStep;
import cascading.management.state.ClientState;

/** Class StepStats collects {@link cascading.flow.FlowStep} specific statistics. */
public abstract class FlowStepStats extends CascadingStats
  {
  private final FlowStep flowStep;

  /** Constructor CascadingStats creates a new CascadingStats instance. */
  protected FlowStepStats( FlowStep flowStep, ClientState clientState )
    {
    super( flowStep.getName(), clientState );
    this.flowStep = flowStep;
    }

  @Override
  public String getID()
    {
    return flowStep.getID();
    }

  protected FlowStep getFlowStep()
    {
    return flowStep;
    }

  @Override
  public synchronized void recordInfo()
    {
    clientState.recordFlowStep( flowStep );
    }

  @Override
  public String toString()
    {
    return "Step{" + getStatsString() + '}';
    }

  public abstract void recordChildStats();
  }
