/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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
 * Enum DebugLevel designates the level of a given {@link cascading.operation.Debug} instance. This is used in conjunction with the
 * {@link cascading.flow.FlowConnector} to plan debug operations out of a particular {@link cascading.flow.Flow} instance.
 * <p>
 * Currently Debug can be denote either DEFAULT or VERBOSE. It is up to the developer to determine if a Debug operation
 * should be at any given level.
 */
public enum DebugLevel implements PlannerLevel
  {
    NONE( 0 ),
    DEFAULT( 1 ),
    VERBOSE( 2 );

  private final int rank;

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