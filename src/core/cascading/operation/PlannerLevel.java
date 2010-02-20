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

package cascading.operation;

/**
 * Interface PlannerLevel is the base interface for {@link DebugLevel} and {@link AssertionLevel} enum types.
 * <p/>
 * It should be noted that all enum implementations of this interface must declare a NONE or equivalent instance. This
 * special case type tells the  {@link cascading.flow.FlowPlanner} to remove all {@link PlannedOperation} instances from the final {@link Flow}.
 *
 * @see DebugLevel
 * @see AssertionLevel
 */
public interface PlannerLevel
  {
  /**
   * Returns true if this enum instance represents a value that instructs the {@link cascading.flow.FlowPlanner} to
   * remove all instances from the plan.
   *
   * @return true if this represents "NONE"
   */
  boolean isNoneLevel();

  /**
   * Returns true if the given {@link PlannerLevel} is less strict than this instance.
   *
   * @param plannerLevel of type PlannerLevel
   * @return true if argument is less strict
   */
  boolean isStricterThan( PlannerLevel plannerLevel );
  }