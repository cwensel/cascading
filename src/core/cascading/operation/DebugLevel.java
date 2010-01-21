/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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
 * Enum DebugLevel designates the level of a given {@link cascading.operation.Debug} instance. This is used in conjuction with the
 * {@link cascading.flow.FlowConnector} to plan dubug operations out of a particular {@link cascading.flow.Flow} instance.
 * </p>
 * Currently Debug can be denote either DEFAULT or VERBOSE. It is up to the developer to determine if a Debug operation
 * should be at any given level.
 */
public enum DebugLevel implements PlannerLevel
  {
    NONE( 0 ),
    DEFAULT( 1 ),
    VERBOSE( 2 );

  private int rank;

  DebugLevel( int rank )
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
    return rank > ( (DebugLevel) plannerLevel ).rank;
    }
  }