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

import cascading.CascadingException;

/** Class OperationException is thrown by {@link BaseOperation} classes. */
public class OperationException extends CascadingException
  {
  /** Constructor OperationException creates a new OperationException instance. */
  public OperationException()
    {
    }

  /**
   * Constructor OperationException creates a new OperationException instance.
   *
   * @param string of type String
   */
  public OperationException( String string )
    {
    super( string );
    }

  /**
   * Constructor OperationException creates a new OperationException instance.
   *
   * @param string    of type String
   * @param throwable of type Throwable
   */
  public OperationException( String string, Throwable throwable )
    {
    super( string, throwable );
    }

  /**
   * Constructor OperationException creates a new OperationException instance.
   *
   * @param throwable of type Throwable
   */
  public OperationException( Throwable throwable )
    {
    super( throwable );
    }
  }