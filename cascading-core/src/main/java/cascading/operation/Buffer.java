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

/**
 * A Buffer is similar to an {@link Aggregator} by the fact that it operates on unique groups of values. It differs
 * by the fact that an {@link java.util.Iterator} is provided and it is the responsibility
 * of the {@link #operate(cascading.flow.FlowProcess, BufferCall)} method to iterate overall all the input
 * arguments returned by this Iterator, if any.
 * <p/>
 * For the case where a Buffer follows a CoGroup, the method {@link #operate(cascading.flow.FlowProcess, BufferCall)}
 * will be called for every unique group whether or not there are values available to iterate over. This may be
 * counter-intuitive for the case of an 'inner join' where the left or right stream may have a null grouping key value.
 * Regardless, the current grouping value can be retrieved through {@link BufferCall#getGroup()}.
 * <p/>
 * Buffer is very useful when header or footer values need to be inserted into a grouping, or if values need to be
 * inserted into the middle of the group values. For example, consider a stream of timestamps. A Buffer could
 * be used to add missing entries, or to calculate running or moving averages over a smaller "window" within the grouping.
 * <p/>
 * By default, if a result is emitted from the Buffer before the argumentsIterator is started or after it is
 * completed ({@code argumentsIterator.hasNext() == false}), non-grouping values are forced to null (to allow for header
 * and footer tuple results).
 * <p/>
 * By setting {@link BufferCall#setRetainValues(boolean)} to {@code true} in the
 * {@link Buffer#prepare(cascading.flow.FlowProcess, OperationCall)} method, the last seen Tuple values will not be
 * nulled after completion and will be treated as the current incoming Tuple when merged with the Buffer result Tuple
 * via the Every outgoing selector.
 * <p/>
 * There may be only one Buffer after a {@link cascading.pipe.GroupBy} or {@link cascading.pipe.CoGroup}. And there
 * may not be any additional {@link cascading.pipe.Every} pipes before or after the buffers Every pipe instance. A
 * {@link cascading.flow.planner.PlannerException} will be thrown if these rules are violated.
 * <p/>
 * Buffer implementations should be re-entrant. There is no guarantee a Buffer instance will be executed in a
 * unique vm, or by a single thread. Also, note the Iterator will return the same {@link cascading.tuple.TupleEntry}
 * instance, but with new values in its child {@link cascading.tuple.Tuple}.
 */
public interface Buffer<Context> extends Operation<Context>
  {
  /**
   * Method operate is called once for each grouping. {@link BufferCall} passes in an {@link java.util.Iterator}
   * that returns an argument {@link cascading.tuple.TupleEntry} for each value in the grouping defined by the
   * argument selector on the parent Every pipe instance.
   * <p/>
   * TupleEntry entry, or entry.getTuple() should not be stored directly in a collection or modified.
   * A copy of the tuple should be made via the {@code new Tuple( entry.getTuple() )} copy constructor.
   * <p/>
   * This method is called for every unique group, whether or not there are values in the arguments Iterator.
   *
   * @param flowProcess of type FlowProcess
   * @param bufferCall  of type BufferCall
   */
  void operate( FlowProcess flowProcess, BufferCall<Context> bufferCall );
  }