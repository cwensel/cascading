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

package cascading.operation;

import java.util.Map;

import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/**
 * An Aggregator takes the set of all values associated with a grouping and returns
 * a single value. Max, Min, Count, and Average are good examples.
 * <p/>
 * An Aggregator is called in the reduce phase.
 * <p/>
 * Aggregator implementations should be reentrant. There is no guarantee an Aggregator instance will be executed in a
 * unique vm, or by a single thread. The given Map context is guaranteed to be unique per 'reducer'.
 */
public interface Aggregator extends Operation
  {
  /**
   * Method start initializes the aggregation procedure.  The {@link Map} context is used to
   * hold intermediate values. The context should be initialized here if necessary. This method will be called
   * before {@link #aggregate(Map, TupleEntry)} and {@link #complete(java.util.Map,cascading.tuple.TupleCollector)}.
   * <p/>
   * TupleEntry groupEntry, or groupEntry.getTuple() should not be stored directly in the context. A copy of the tuple
   * should be made via the {@code new Tuple( entry.getTuple() )} copy constructor.
   *
   * @param context    the map to be initialized (if necessary)
   * @param groupEntry is the current grouping tuple
   */
  void start( Map context, TupleEntry groupEntry );

  /**
   * Method aggregate is called for each {@link TupleEntry} value in the current grouping.
   * <p/>
   * TupleEntry entry, or entry.getTuple() should not be stored directly in the context. A copy of the tuple
   * should be made via the {@code new Tuple( entry.getTuple() )} copy constructor.
   *
   * @param context the map with aggregate values so far
   * @param entry   the tuple entry to add to the operation
   */
  void aggregate( Map context, TupleEntry entry );

  /**
   * Method complete will be issued last after every {@link TupleEntry} has been passed to the
   * {@link #aggregate(Map, TupleEntry)} method.  Any final calculation should be completed
   * here and passed to the outputCollector.
   *
   * @param context         the aggregate map @return the final aggregate value
   * @param outputCollector
   */
  void complete( Map context, TupleCollector outputCollector );
  }