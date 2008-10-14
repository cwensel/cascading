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

import cascading.tuple.TupleEntry;

/** Interface GroupAssertionCall provides access to the current {@link GroupAssertion} invocation arguments. */
public interface GroupAssertionCall<C> extends OperationCall<C>
  {
  /**
   * Returns the user set context object, C. Will return null if {@link #setContext(Object)} was not called
   * during {@link GroupAssertion#start(cascading.flow.FlowProcess, GroupAssertionCall)}.
   *
   * @return user defined object
   */
  C getContext();

  /**
   * Sets the 'context' object used by code in {@link GroupAssertion#aggregate(cascading.flow.FlowProcess, GroupAssertionCall)}.
   * <p/>
   * This method should only be called in the {@link GroupAssertion#start(cascading.flow.FlowProcess, GroupAssertionCall)}
   * method. Further, if {@link #getContext()} does not return null, consider 'resetting' the current instance. For
   * example, if the 'context' is a Map or Set, call the clear() method instead of creating a new Map instance.
   *
   * @param context user defined object
   */
  void setContext( C context );

  /**
   * Returns the current grouping {@link TupleEntry}.
   *
   * @return TupleEnry
   */
  TupleEntry getGroup();

  /**
   * Returns {@link TupleEntry} of argument values.
   *
   * @return TupleEntry
   */
  TupleEntry getArguments();
  }