/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * The GroupBy pipe groups the {@link Tuple} stream by the given groupFields.
 * </p>
 * If more than one {@link Pipe} instance is provided on the constructor, all branches will be merged. It is required
 * that all Pipe instances output the same field names, otherwise the {@link FlowConnector} will fail to create a
 * {@link Flow} instance. Again, the Pipe instances are merged together as if one Tuple stream and not joined.
 * See {@link CoGroup} for joining by common fields.
 * </p>
 * Typically an {@link Every} follows GroupBy to apply an {@link Aggregator} function to every grouping. The
 * {@link Each} operator may also follow GroupBy to apply a {@link Function} or {@link Filter} to the resulting
 * stream. But an Each cannot come immediately before an Every.
 * <p/>
 * Optionally a stream can be further sorted by providing sortFields. This allows an Aggregator to receive
 * values in the order of the sortedFields.
 * <p/>
 * Note that local sorting always happens on the groupFields, sortFields are a secondary sorting on the grouped values within the
 * current grouping. sortFields is particularly useful if the Aggregators following the GroupBy would like to see their arguments
 * in order.
 * <p/>
 * It should be noted for MapReduce systems, distributed group sorting is not 'complete'. That is groups are sorted
 * as seen by each Reducer, but they are not sorted across Reducers. See the MapReduce algorithm for details.
 */
public class GroupBy extends Group
  {
  /**
   * Creates a new GroupBy instance that will group on {@link Fields#ALL} fields.
   *
   * @param pipe of type Pipe
   */
  public GroupBy( Pipe pipe )
    {
    super( pipe );
    }

  /**
   * Creates a new GroupBy instance that will group on the given groupFields field names.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   */
  public GroupBy( Pipe pipe, Fields groupFields )
    {
    super( pipe, groupFields );
    }

  /**
   * Creates a new GroupBy instance that will group on the given groupFields field names.
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
   * Creates a new GroupBy instance that will group on the given groupFields field names
   * and sorts the grouped values on the given sortFields fields names.
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
   * Creates a new GroupBy instance that will group on the given groupFields field names
   * and sorts the grouped values on the given sortFields fields names.
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
   * Creates a new GroupBy instance that will group on the given groupFields field names
   * and sorts the grouped values on the given sortFields fields names.
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
   * Creates a new GroupBy instance that will group on the given groupFields field names
   * and sorts the grouped values on the given sortFields fields names.
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

  //////////
  // MERGE
  //////////

  /**
   * Creates a new GroupBy instance that will first merge the given pipes, then group on Fields.FIRST.
   * <p/>
   * The assumption is that the first fields in all streams are logically the same field, which should be true
   * as merging assumes all incoming streams have the same fields in the same order.
   *
   * @param pipes of type Pipe
   */
  public GroupBy( Pipe[] pipes )
    {
    super( pipes, Fields.FIRST );
    }

  /**
   * Creates a new GroupBy instance that will first merge the given pipes, then group on the given groupFields field names.
   *
   * @param pipes       of type Pipe
   * @param groupFields of type Fields
   */
  public GroupBy( Pipe[] pipes, Fields groupFields )
    {
    super( pipes, groupFields );
    }

  /**
   * Creates a new GroupBy instance that will first merge the given pipes, then group on the given groupFields field names.
   *
   * @param groupName   of type String
   * @param pipes       of type Pipe
   * @param groupFields of type Fields
   */
  public GroupBy( String groupName, Pipe[] pipes, Fields groupFields )
    {
    super( groupName, pipes, groupFields );
    }

  /**
   * Creates a new GroupBy instance that will first merge the given pipes, then group on the given groupFields field names
   * and sorts the grouped values on the given sortFields fields names.
   *
   * @param pipes       of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  public GroupBy( Pipe[] pipes, Fields groupFields, Fields sortFields )
    {
    super( pipes, groupFields, sortFields );
    }

  /**
   * Creates a new GroupBy instance that will first merge the given pipes, then group on the given groupFields field names
   * and sorts the grouped values on the given sortFields fields names.
   *
   * @param groupName   of type String
   * @param pipes       of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  public GroupBy( String groupName, Pipe[] pipes, Fields groupFields, Fields sortFields )
    {
    super( groupName, pipes, groupFields, sortFields );
    }

  /**
   * Creates a new GroupBy instance that will first merge the given pipes, then group on the given groupFields field names
   * and sorts the grouped values on the given sortFields fields names.
   *
   * @param pipes        of type Pipe
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  public GroupBy( Pipe[] pipes, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    super( pipes, groupFields, sortFields, reverseOrder );
    }

  /**
   * Creates a new GroupBy instance that will first merge the given pipes, then group on the given groupFields field names
   * and sorts the grouped values on the given sortFields fields names.
   *
   * @param groupName    of type String
   * @param pipes        of type Pipe
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  public GroupBy( String groupName, Pipe[] pipes, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    super( groupName, pipes, groupFields, sortFields, reverseOrder );
    }
  }
