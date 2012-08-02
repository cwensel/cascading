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

package cascading.operation;

import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntry;

/**
 * Class GroupAssertion is a kind of {@link Assertion} used with the {@link cascading.pipe.Every} pipe Operator.
 * <p/>
 * Implementers should also extend {@link BaseOperation}.
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