/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.operation;

import cascading.CascadingException;

/** Class OperationException is thrown by {@link Operation} classes. */
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