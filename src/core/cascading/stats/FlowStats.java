/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import cascading.flow.Flow;


/** Class FlowStats collects {@link Flow} specific statistics. */
public class FlowStats extends CascadingStats
  {
  String flowID;
  List<StepStats> stepStatsList = new ArrayList<StepStats>();

  public FlowStats( String flowName, String flowID )
    {
    super( flowName );
    this.flowID = flowID;
    }

  @Override
  public Object getID()
    {
    return flowID;
    }

  public void addStepStats( StepStats stepStats )
    {
    stepStatsList.add( stepStats );
    }

  /**
   * Method getStepStats returns the stepStats owned by this FlowStats.
   *
   * @return the stepStats (type List<StepStats>) of this FlowStats object.
   */
  public List<StepStats> getStepStats()
    {
    return stepStatsList;
    }

  /**
   * Method getStepsCount returns the number of steps this Flow executed.
   *
   * @return the stepsCount (type int) of this FlowStats object.
   */
  public int getStepsCount()
    {
    return stepStatsList.size();
    }

  @Override
  public long getCounterValue( Enum counter )
    {
    long value = 0;

    for( StepStats step : stepStatsList )
      value += step.getCounterValue( counter );

    return value;
    }

  @Override
  public long getCounterValue( String group, String counter )
    {
    long value = 0;

    for( StepStats step : stepStatsList )
      value += step.getCounterValue( group, counter );

    return value;
    }

  @Override
  public void captureDetail()
    {
    for( StepStats stepStats : stepStatsList )
      stepStats.captureDetail();
    }

  @Override
  public Collection getChildren()
    {
    return getStepStats();
    }

  @Override
  protected String getStatsString()
    {
    return super.getStatsString() + ", stepsCount=" + getStepsCount();
    }

  @Override
  public String toString()
    {
    return "Flow{" + getStatsString() + '}';
    }
  }
