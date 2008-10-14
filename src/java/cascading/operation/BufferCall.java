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

import java.util.Iterator;

import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/** Interface BufferCall provides access to the current {@link cascading.operation.Buffer} invocation arguments. */
public interface BufferCall
  {
  /**
   * Returns the current grouping {@link cascading.tuple.TupleEntry}.
   *
   * @return TupleEnry
   */
  TupleEntry getGroup();

  /**
   * Returns an {@link Iterator} of {@link TupleEntry} instances representing the arguments for the called
   * {@link Buffer#operate(cascading.flow.FlowProcess, BufferCall)} method.
   *
   * @return Iterator<TupleEntry>
   */
  Iterator<TupleEntry> getArgumentsIterator();


  /**
   * Returns the {@link cascading.tuple.TupleCollector} used to emit result values. Zero or more entries may be emitted.
   *
   * @return TupleCollector
   */
  TupleCollector getOutputCollector();
  }