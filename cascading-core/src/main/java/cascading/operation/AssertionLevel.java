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

package cascading.operation;

/**
 * Enum AssertionLevel designates the level of a given {@link Assertion} instance. This is used in conjunction with the
 * {@link cascading.flow.FlowConnector} to plan assertions out of a particular {@link cascading.flow.Flow} instance.
 * </p>
 * Currently Assertions can be denote either VALID or STRICT.
 * </p>
 * VALID assertions are used to validate data during staging testing or for use in a production environment.
 * </p>
 * STRICT assertions should be used as unit test would be against regression data and during development.
 */
public enum AssertionLevel implements PlannerLevel
  {
    NONE( 0 ),
    VALID( 1 ),
    STRICT( 2 );

  private final int rank;

  AssertionLevel( int rank )
    {
    this.rank = rank;
    }

  @Override
  public boolean isNoneLevel()
    {
    return this == NONE;
    }

  @Override
  public boolean isStricterThan( PlannerLevel plannerLevel )
    {
    return rank > ( (AssertionLevel) plannerLevel ).rank;
    }
  }
