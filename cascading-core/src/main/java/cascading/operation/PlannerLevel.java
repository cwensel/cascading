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

package cascading.operation;

/**
 * Interface PlannerLevel is the base interface for {@link DebugLevel} and {@link AssertionLevel} enum types.
 * <p/>
 * It should be noted that all enum implementations of this interface must declare a NONE or equivalent instance. This
 * special case type tells the  {@link cascading.flow.planner.FlowPlanner} to remove all {@link PlannedOperation} instances from
 * the final {@link cascading.flow.Flow}.
 *
 * @see DebugLevel
 * @see AssertionLevel
 */
public interface PlannerLevel
  {
  /**
   * Returns true if this enum instance represents a value that instructs the {@link cascading.flow.planner.FlowPlanner} to
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