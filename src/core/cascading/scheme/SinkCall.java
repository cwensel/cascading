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

package cascading.scheme;

import cascading.tuple.TupleEntry;

/**
 * SinkCall provides access to the current {@link Scheme#sink(cascading.flow.FlowProcess, SinkCall)} invocation
 * arguments.
 * <p/>
 * Use the Context to store thread local values.
 *
 * @param <Context>
 * @param <Output>
 */
public interface SinkCall<Context, Output>
  {
  /**
   * Method getContext returns the context of this SinkCall object.
   *
   * @return the context (type C) of this SinkCall object.
   */
  Context getContext();

  /**
   * Method setContext sets the context of this SinkCall object.
   *
   * @param context the context of this SinkCall object.
   */
  void setContext( Context context );

  /**
   * Method getOutgoingEntry returns the final {@link TupleEntry} to be passed to the
   * {@link #getOutput()} output handler.
   * <p/>
   * That is, the result of calling getOutgoingEntry() should be passed directly to the
   * platform specific output handler returned by getOutput().
   * <p/>
   * Note the returned value from this method cannot be modified.
   *
   * @return TupleEntry
   */
  TupleEntry getOutgoingEntry();

  Output getOutput();
  }
