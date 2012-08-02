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

import cascading.tuple.TupleEntry;

/**
 * Interface GroupAssertionCall provides access to the current {@link GroupAssertion} invocation arguments.
 * <p/>
 * This interface is generic, allowing the user to set a custom 'context' object when {@link GroupAssertion#start(cascading.flow.FlowProcess, GroupAssertionCall)}
 * is called. The {@link OperationCall#setContext(Object)} and {@link OperationCall#getContext()} methods are
 * inherited from {@link OperationCall}.
 *
 * @see GroupAssertion
 * @see OperationCall
 */
public interface GroupAssertionCall<C> extends OperationCall<C>
  {
  /**
   * Returns the current grouping {@link TupleEntry}.
   *
   * @return TupleEntry
   */
  TupleEntry getGroup();

  /**
   * Returns {@link TupleEntry} of argument values.
   *
   * @return TupleEntry
   */
  TupleEntry getArguments();
  }