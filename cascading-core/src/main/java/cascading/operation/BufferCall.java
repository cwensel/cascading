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

import java.util.Iterator;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/** Interface BufferCall provides access to the current {@link cascading.operation.Buffer} invocation arguments. */
public interface BufferCall<C> extends OperationCall<C>
  {
  /**
   * Returns the current grouping {@link cascading.tuple.TupleEntry}.
   *
   * @return TupleEntry
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
   * Return the resolved {@link cascading.tuple.Fields} declared by the current {@link Operation}.
   *
   * @return Fields
   */
  Fields getDeclaredFields();

  /**
   * Returns the {@link cascading.tuple.TupleEntryCollector} used to emit result values. Zero or more entries may be emitted.
   *
   * @return TupleCollector
   */
  TupleEntryCollector getOutputCollector();

  /**
   * Set to {@code false} if at the end of all values iterated over in the argumentsIterator, the last seen argument tuple
   * values should not be nulled out.
   * <p/>
   * By default, if a result is emitted from the Buffer before and after the argumentsIterator is started or completed,
   * the last seen non-grouping values are null. When false, the values are not nulled after completion.
   * <p/>
   * The default is {@code true}.
   *
   * @param retainValues of type boolean
   */
  void setRetainValues( boolean retainValues );

  /**
   * Returns {@code true} if non-grouping fields will not be nulled after the argumentsIterator is completed.
   *
   * @return true
   */
  boolean isRetainValues();
  }