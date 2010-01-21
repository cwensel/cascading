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

package cascading.operation;

import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntry;

/**
 * Class GroupAssertion is a kind of {@link Assertion} used with the {@link cascading.pipe.Every} pipe Operator.
 * <p/>
 * Implementors should also extend {@link BaseOperation}.
 *
 * @see Aggregator
 */
public interface GroupAssertion<C> extends Assertion<C>
  {
  /**
   * Method start initializes the aggregation procedure and is called for every unique grouping.
   * <p/>
   * The AggregatorCall context should be initialized here if necessary.
   * <p/>
   * The first time this method is called for a given 'process', the AggregatorCall context will be null. This method should
   * set a new instance of the user defined context object. When the AggregatorCall context is not null, it is up to
   * the developer to create a new instance, or 'recycle' the given instance. If recycled, it must be re-initialized to
   * remove any previous state/values.
   * <p/>
   * For example, if a Map is used to hold the intermediate data for each subsequent
   * {@link #aggregate(cascading.flow.FlowProcess, GroupAssertionCall)} call,
   * new HashMap() should be set on the AggregatorCall instance when {@link cascading.operation.AggregatorCall#getContext()} is null.
   * On the next grouping, start() will be called again, but this time with the old Map instance. In this case,
   * map.clear() should be invoked before returning.
   *
   * @param flowProcess   of type FlowProcess
   * @param assertionCall of type GroupAssertionCall
   */
  void start( FlowProcess flowProcess, GroupAssertionCall<C> assertionCall );

  /**
   * Method aggregate is called for each {@link TupleEntry} value in the current grouping.
   *
   * @param flowProcess   of type FlowProcess
   * @param assertionCall of type GroupAssertionCall
   */
  void aggregate( FlowProcess flowProcess, GroupAssertionCall<C> assertionCall );

  /**
   * Method doAssert performs the assertion.
   *
   * @param flowProcess   of type FlowProcess
   * @param assertionCall of type GroupAssertionCall
   */
  void doAssert( FlowProcess flowProcess, GroupAssertionCall<C> assertionCall );

  }