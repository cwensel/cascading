/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.stats;

import cascading.flow.planner.FlowStep;
import cascading.management.ClientState;

/** Class StepStats collects {@link cascading.flow.planner.FlowStep} specific statistics. */
public abstract class StepStats extends CascadingStats
  {
  private FlowStep flowStep;

  /** Constructor CascadingStats creates a new CascadingStats instance. */
  protected StepStats( FlowStep flowStep, ClientState clientState )
    {
    super( flowStep.getName(), clientState );
    this.flowStep = flowStep;
    }

  @Override
  public String getID()
    {
    return flowStep.getID();
    }

  @Override
  public void recordInfo()
    {
    clientState.recordFlowStep( flowStep );
    }

  @Override
  public String toString()
    {
    return "Step{" + getStatsString() + '}';
    }

  public abstract void captureJobStats();
  }
