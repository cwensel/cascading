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

package cascading.tuple;

import cascading.operation.Operation;

/** Interface TupleCollector is used to allow {@link Operation} instances to emit result {@link Tuple} values. */
public interface TupleCollector
  {
  /**
   * Method add inserts the given {@link TupleEntry} into the outgoing stream.
   *
   * @param entry of type TupleEntry
   */
  void add( TupleEntry entry );

  /**
   * Method add inserts the given {@link Tuple} into the outgoing stream.
   *
   * @param tuple of type Tuple
   */
  void add( Tuple tuple );
  }