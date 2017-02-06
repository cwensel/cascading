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

package cascading.cascade;

import cascading.CascadingException;

/** Class CascadeException is thrown by the {@link Cascade} class. */
public class CascadeException extends CascadingException
  {
  /** Constructor CascadeException creates a new CascadeException instance. */
  public CascadeException()
    {
    }

  /**
   * Constructor CascadeException creates a new CascadeException instance.
   *
   * @param string of type String
   */
  public CascadeException( String string )
    {
    super( string );
    }

  /**
   * Constructor CascadeException creates a new CascadeException instance.
   *
   * @param string    of type String
   * @param throwable of type Throwable
   */
  public CascadeException( String string, Throwable throwable )
    {
    super( string, throwable );
    }

  /**
   * Constructor CascadeException creates a new CascadeException instance.
   *
   * @param throwable of type Throwable
   */
  public CascadeException( Throwable throwable )
    {
    super( throwable );
    }
  }
