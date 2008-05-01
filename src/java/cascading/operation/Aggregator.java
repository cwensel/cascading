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

package cascading.operation;

import java.util.Map;

import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/**
 * An Aggregator takes the set of all values associated with a grouping and returns
 * a single value. Max, Min, Count, and Average are good examples.
 * <p/>
 * An Aggregator is called in the reduce phase.
 */
public interface Aggregator
  {
  /**
   * Initializes the aggregation procedure.  The {@link Map} is the utility that
   * holds values to be aggregated.  If an initial value should be set here, this
   * method should be called before {@link #aggregate(Map, TupleEntry)} and {@link #complete(java.util.Map,cascading.tuple.TupleCollector)}.
   *
   * @param context    the map to be initialized (if necessary)
   * @param groupEntry is the current grouping tuple
   */
  void start( Map context, TupleEntry groupEntry );

  /**
   * Adds a {@link TupleEntry} to the aggregation operation.  This is issued by
   * reducer for each {@link TupleEntry} that should partake in the aggregation
   * operation.
   *
   * @param context the map with aggregate values so far
   * @param entry   the tuple entry to add to the operation
   */
  void aggregate( Map context, TupleEntry entry );

  /**
   * This should be issued last after each {@link TupleEntry} has been added by the
   * {@link #aggregate(Map, TupleEntry)} operation.  Any final calculation is done
   * here and a single {@link Tuple} is returned with the aggregate value.
   *
   * @param context         the aggregate map @return the final aggregate value
   * @param outputCollector
   */
  void complete( Map context, TupleCollector outputCollector );
  }