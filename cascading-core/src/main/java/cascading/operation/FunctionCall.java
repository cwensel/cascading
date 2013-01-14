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

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/** Interface FunctionCall provides access to the current {@link Function} invocation arguments. */
public interface FunctionCall<C> extends OperationCall<C>
  {
  /**
   * Returns {@link TupleEntry} of argument values.
   *
   * @return TupleEntry
   */
  TupleEntry getArguments();

  /**
   * Return the resolved {@link Fields} declared by the current {@link Operation}.
   *
   * @return Fields
   */
  Fields getDeclaredFields();

  /**
   * Returns the {@link cascading.tuple.TupleEntryCollector} used to emit result values.
   *
   * @return TupleCollector
   */
  TupleEntryCollector getOutputCollector();
  }
