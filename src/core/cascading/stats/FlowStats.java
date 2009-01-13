/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.Flow;


/** Class FlowStats collects {@link Flow} specific statistics. */
public class FlowStats extends CascadingStats
  {
  /** Field stepsCount */
  int stepsCount;

  /**
   * Method getStepsCount returns the number of steps this Flow executed.
   *
   * @return the stepsCount (type int) of this FlowStats object.
   */
  public int getStepsCount()
    {
    return stepsCount;
    }

  /**
   * Method setStepsCount sets the steps value.
   *
   * @param stepsCount the stepsCount of this FlowStats object.
   */
  public void setStepsCount( int stepsCount )
    {
    this.stepsCount = stepsCount;
    }

  @Override
  protected String getStatsString()
    {
    return super.getStatsString() + ", stepsCount=" + stepsCount;
    }

  @Override
  public String toString()
    {
    return "Flow{" + getStatsString() + '}';
    }
  }
