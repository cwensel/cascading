/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe.cogroup;

import java.io.Serializable;
import java.util.Iterator;

import cascading.tuple.Tuple;

/** Interface Joiner allows for custom join strategies against a {@link CoGroupClosure}. */
public interface Joiner extends Serializable
  {
  /**
   * Returns an iterator that joins the given CoGroupClosure co-groups.
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