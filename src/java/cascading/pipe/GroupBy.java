/*
 * Copyright (c) 2007-2008 Chris K Wensel. All Rights Reserved.
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
 * <p/>
 * Optionally a stream can be further sorted by providing sortFields. This allows an Aggregator to receive
 * values in the order of the sortedFields.
 * <p/>
 * Note that sorting always happens on the groupFields, sortFields are a secondary sorting within the
 * current grouping. sortFields is particularly useful if the Aggregators following the GroupBy would like to see thier argument
 * in order.
 */
public class GroupBy extends Group
  {
  /**
   * Constructor Group creates a new Group instance that will group on {@link Fields#ALL} fields.
   *
   * @param pipe of type Pipe
   */
  public GroupBy( Pipe pipe )
    {
    super( pipe );
    }

  /**
   * Constructor Group creates a new Group instance that will group on the given groupFields field names.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   */
  public GroupBy( Pipe pipe, Fields groupFields )
    {
    super( pipe, groupFields );
    }

  /**
   * Constructor Group creates a new Group instance that will group on the given groupFields field names.
   *
   * @param groupName   of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   */
  public GroupBy( String groupName, Pipe pipe, Fields groupFields )
    {
    super( groupName, pipe, groupFields );
    }

  /**
   * Constructor Group creates a new Group instance that will group on the given groupFields field names
   * and sort on the given sortFields fields names.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  public GroupBy( Pipe pipe, Fields groupFields, Fields sortFields )
    {
    super( pipe, groupFields, sortFields );
    }

  /**
   * Constructor Group creates a new Group instance that will group on the given groupFields field names
   * and sort on the given sortFields fields names.
   *
   * @param groupName   of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  public GroupBy( String groupName, Pipe pipe, Fields groupFields, Fields sortFields )
    {
    super( groupName, pipe, groupFields, sortFields );
    }

  /**
   * Constructor Group creates a new Group instance that will group on the given groupFields field names
   * and sort on the given sortFields fields names.
   *
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  public GroupBy( Pipe pipe, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    super( pipe, groupFields, sortFields, reverseOrder );
    }

  /**
   * Constructor Group creates a new Group instance that will group on the given groupFields field names
   * and sort on the given sortFields fields names.
   *
   * @param groupName    of type String
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  public GroupBy( String groupName, Pipe pipe, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    super( groupName, pipe, groupFields, sortFields, reverseOrder );
    }
  }
