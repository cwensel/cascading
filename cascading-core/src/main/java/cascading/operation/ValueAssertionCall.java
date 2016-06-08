/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

import cascading.tuple.TupleEntry;

/** Interface ValueAssertionCall provides access to the current {@link ValueAssertion} invocation arguments. */
public interface ValueAssertionCall<C> extends OperationCall<C>
  {
  /**
   * Returns {@link TupleEntry} of argument values.
   * <p/>
   * Note that the returned TupleEntry should not be cached (stored in a Collection), nor should the underlying Tuple
   * instance. Where possible Cascading will re-use both TupleEntry and Tuple instances.
   * <p/>
   * To get a safe copy that can be cached, use {@link TupleEntry#getTupleCopy()}.
   *
   * @return TupleEntry
   */
  TupleEntry getArguments();
  }