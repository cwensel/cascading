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

import cascading.tuple.TupleEntry;

/**
 *
 */
public interface GroupAssertion extends Assertion
  {
  /**
   * Method start initializes the grouping procedure.  The {@link java.util.Map} context is used to
   * hold intermediate values. The context should be initialized here if necessary. This method will be called
   * before {@link #aggregate(java.util.Map , cascading.tuple.TupleEntry)} and {@link #doAssert(java.util.Map)}.
   *
   * @param context    the map to be initialized (if necessary)
   * @param groupEntry is the current grouping tuple
   */
  void start( Map context, TupleEntry groupEntry );

  /**
   * Method aggregate is called for each {@link TupleEntry} value in the current grouping.
   *
   * @param context the map with aggregate values so far
   * @param entry   the tuple entry to add to the operation
   */
  void aggregate( Map context, TupleEntry entry );

  /**
   * Method doAssert performs the assertion.
   *
   * @param context of type Map
   */
  void doAssert( Map context );

  }