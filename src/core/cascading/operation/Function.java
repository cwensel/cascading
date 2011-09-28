/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

/** Interface Function marks a given {@link BaseOperation} as a function, as opposed to being a {@link Filter}. */
public interface Function<Context> extends Operation<Context>
  {
  /**
   * Method operate provides the implementation of this Function.
   *
   * @param flowProcess  of type FlowProcess
   * @param functionCall of type FunctionCall
   */
  void operate( FlowProcess flowProcess, FunctionCall<Context> functionCall );
  }