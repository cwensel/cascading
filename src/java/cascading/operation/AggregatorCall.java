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

import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/**
 * Interface AggregatorCall provides access to the current {@link Aggregator} invocation arguments.
 * <p/>
 * This interface is generic, allowing the user to set a custom 'context' object when {@link Aggregator#start(cascading.flow.FlowProcess, AggregatorCall)}
 * is called.
 */
public interface AggregatorCall<C> extends OperationCall<C>
  {
  /**
   * Returns the user set context object, C. Will return null if {@link #setContext(Object)} was not called
   * during {@link Aggregator#start(cascading.flow.FlowProcess, AggregatorCall}.
   *
   * @return user defined object
   */
  C getContext();

  /**
   * Sets the 'context' object used by code in {@link Aggregator#aggregate(cascading.flow.FlowProcess, AggregatorCall)}.
   * <p/>
   * This method should only be called in the {@link Aggregator#start(cascading.flow.FlowProcess, AggregatorCall)}
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
   * <p/>
   * Will return {@code null} unless called in {@link Aggregator#aggregate(cascading.flow.FlowProcess, AggregatorCall)}.
   *
   * @return TupleEntry
   */
  TupleEntry getArguments();

  /**
   * Returns the {@link TupleCollector} used to emit result values.
   * <p/>
   * Will return {@code null} unless called in {@link Aggregator#complete(cascading.flow.FlowProcess, AggregatorCall)}.
   *
   * @return TupleCollector
   */
  TupleCollector getOutputCollector();
  }
