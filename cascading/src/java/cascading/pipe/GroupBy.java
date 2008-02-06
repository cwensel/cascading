/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

package cascading.pipe;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

/**
 * The GroupBy pipe groups the Tuple stream by the given groupFields. Typically an {@link Every} follows GroupBy to apply an
 * {@link Aggregator} function to every grouping.
 */
public class GroupBy extends Group
  {
  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe of type Pipe
   */
  public GroupBy( Pipe pipe )
    {
    super( pipe );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   */
  public GroupBy( Pipe pipe, Fields groupFields )
    {
    super( pipe, groupFields );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName   of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   */
  public GroupBy( String groupName, Pipe pipe, Fields groupFields )
    {
    super( groupName, pipe, groupFields );
    }
  }
