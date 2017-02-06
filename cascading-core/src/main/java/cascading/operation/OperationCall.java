/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

public interface OperationCall<Context>
  {
  /**
   * Returns the user set context object, C.
   *
   * @return user defined object
   */
  Context getContext();

  /**
   * Sets the user defined 'context' object.
   *
   * @param context user defined object
   */
  void setContext( Context context );

  /**
   * Returns the {@link Fields} of the expected arguments {@link cascading.tuple.TupleEntry}.
   *
   * @return the argumentFields (type Fields) of this OperationCall object.
   */
  Fields getArgumentFields();
  }