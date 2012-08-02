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
 * SourceCall provides access to the current {@link Scheme#source(cascading.flow.FlowProcess, SourceCall)} invocation
 * arguments.
 * <p/>
 * Use the Context to store thread local values.
 *
 * @param <Context>
 * @param <Input>
 */
public interface SourceCall<Context, Input>
  {
  /**
   * Method getContext returns the context of this SourceCall object.
   *
   * @return the context (type C) of this SourceCall object.
   */
  Context getContext();

  /**
   * Method setContext sets the context of this SourceCall object.
   *
   * @param context the context of this SourceCall object.
   */
  void setContext( Context context );

  /**
   * Method getIncomingEntry returns a pre-prepared {@link TupleEntry} to be populated
   * with the input values from {@link #getInput()}.
   * <p/>
   * That is, using the getInput() method, retrieve the current incoming values and
   * place them into the getIncomingEntry() via {@link TupleEntry#setTuple(cascading.tuple.Tuple)}
   * or by modifying the tuple returned from {@link cascading.tuple.TupleEntry#getTuple()}.
   * <p/>
   * The returned Tuple entry is guaranteed to be the size of the declared incoming source fields.
   * <p/>
   * The returned TupleEntry from this method is modifiable and is intended to be re-used. This is an exception to
   * the general rule that passed TupleEntry instances must not be modified.
   *
   * @return TupleEntry
   */
  TupleEntry getIncomingEntry();

  /**
   * Method getInput returns the input mechanism for the underlying platform used to retrieve new values (records,
   * lines, etc).
   * <p/>
   * Do not cache the returned value as it may change.
   *
   * @return the platform dependent input handler
   */
  Input getInput();
  }
