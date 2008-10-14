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

import cascading.flow.FlowSession;

/**
 * A Buffer takes the set of all values associated with a grouping and returns
 * zero or more values.
 * <p/>
 * Buffer implementations should be reentrant. There is no guarantee an Buffer instance will be executed in a
 * unique vm, or by a single thread. The {@link #start(cascading.flow.FlowSession, BufferCall)}
 * method provides a mechanism for maintaining a 'context' object to hold intermedite values.
 */
public interface Buffer<C> extends Operation
  {
  /**
   * Method operate is called for each {@link cascading.tuple.TupleEntry} value in the current grouping.
   * <p/>
   * TupleEntry entry, or entry.getTuple() should not be stored directly in the context. A copy of the tuple
   * should be made via the {@code new Tuple( entry.getTuple() )} copy constructor.
   *
   * @param flowSession    of type FlowSession is the current session
   * @param aggregatorCall
   */
  void operate( FlowSession flowSession, BufferCall bufferCall );
  }