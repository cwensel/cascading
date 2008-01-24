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

package cascading.flow;

import cascading.CascadingException;

/** FlowException instances are thrown on errors when building a Flow or when executing one. */
public class FlowException extends CascadingException
  {
  /** Constructor FlowException creates a new FlowException instance. */
  public FlowException()
    {
    }

  /**
   * Constructor FlowException creates a new FlowException instance.
   *
   * @param string of type String
   */
  public FlowException( String string )
    {
    super( string );
    }

  /**
   * Constructor FlowException creates a new FlowException instance.
   *
   * @param string    of type String
   * @param throwable of type Throwable
   */
  public FlowException( String string, Throwable throwable )
    {
    super( string, throwable );
    }

  /**
   * Constructor FlowException creates a new FlowException instance.
   *
   * @param throwable of type Throwable
   */
  public FlowException( Throwable throwable )
    {
    super( throwable );
    }
  }
