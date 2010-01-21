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

package cascading.pipe.cogroup;

import java.io.Serializable;
import java.util.Iterator;

import cascading.tuple.Tuple;

/** Interface Joiner allows for custom join strategies against a {@link CoGroupClosure}. */
public interface Joiner extends Serializable
  {
  /**
   * Returns an interator that joins the given CoGroupClosure co-groups.
   *
   * @param closure of type GroupClosure
   * @return an iterator
   */
  Iterator<Tuple> getIterator( GroupClosure closure );

  /**
   * Returns the number of joins this instance can handle. A value of -1 denotes there is no limit.
   *
   * @return an int
   */
  int numJoins();
  }