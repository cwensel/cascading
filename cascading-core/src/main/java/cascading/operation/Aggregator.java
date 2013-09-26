/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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
 * An Aggregator takes the set of all values associated with a unique grouping and returns
 * zero or more values. {@link cascading.operation.aggregator.MaxValue}, {@link cascading.operation.aggregator.MinValue},
 * {@link cascading.operation.aggregator.Count}, and {@link cascading.operation.aggregator.Average} are good examples.
 * <p/>
 * Aggregator implementations should be reentrant. There is no guarantee an Aggregator instance will be executed in a
 * unique vm, or by a single thread. The {@link #start(cascading.flow.FlowProcess, AggregatorCall)}
 * method provides a mechanism for maintaining a 'context' object to hold intermediate values.
 * <p/>
 * Note {@link TupleEntry} instances are reused internally so should not be stored. Instead use the TupleEntry or Tuple
 * copy constructors to make safe copies.
 * <p/>
 * Since Aggregators can be chained, and Cascading pipelines all operation results, any Aggregators
 * coming ahead of the current Aggregator must return a value before the {@link #complete(cascading.flow.FlowProcess, AggregatorCall)}
 * method on this Aggregator is called. Subsequently, if any previous Aggregators return more than one Tuple result,
 * this complete() method will be called for each Tuple emitted.
 * <p/>
 * Thus it is a best practice to implement a {@link Buffer} when emitting more than one, or zero Tuple results.
 *
 * @see AggregatorCall
 * @see OperationCall
 */
public interface Aggregator<Context> extends Operation<Context>
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
   * {@link #aggregate(cascading.flow.FlowProcess, AggregatorCall)} call,
   * new HashMap() should be set on the AggregatorCall instance when {@link cascading.operation.AggregatorCall#getContext()} is null.
   * On the next grouping, start() will be called again, but this time with the old Map instance. In this case,
   * map.clear() should be invoked before returning.
   *
   * @param flowProcess    of type FlowProcess
   * @param aggregatorCall of type AggregatorCall
   */
  void start( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall );

  /**
   * Method aggregate is called for each {@link TupleEntry} value in the current grouping.
   * <p/>
   * TupleEntry entry, or entry.getTuple() should not be stored directly in the context. A copy of the tuple
   * should be made via the {@code new Tuple( entry.getTuple() )} copy constructor.
   *
   * @param flowProcess    of type FlowProcess
   * @param aggregatorCall of type AggregatorCall
   */
  void aggregate( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall );

  /**
   * Method complete will be issued last after every {@link TupleEntry} has been passed to the
   * {@link #aggregate(cascading.flow.FlowProcess, AggregatorCall)}
   * method.  Any final calculation should be completed here and passed to the outputCollector.
   *
   * @param flowProcess    of type FlowProcess
   * @param aggregatorCall of type AggregatorCall
   */
  void complete( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall );
  }