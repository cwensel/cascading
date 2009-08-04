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

package cascading.operation;

/**
 * Enum AssertionLevel designates the level of a given {@link Assertion} instance. This is used in conjuction with the
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

  private int rank;

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
