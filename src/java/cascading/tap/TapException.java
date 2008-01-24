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

package cascading.tap;

import cascading.CascadingException;

/** Class TapException is thrown from {@link Tap} subclasses. */
public class TapException extends CascadingException
  {
  /** Constructor TapException creates a new TapException instance. */
  public TapException()
    {
    }

  /**
   * Constructor TapException creates a new TapException instance.
   *
   * @param string of type String
   */
  public TapException( String string )
    {
    super( string );
    }

  /**
   * Constructor TapException creates a new TapException instance.
   *
   * @param string    of type String
   * @param throwable of type Throwable
   */
  public TapException( String string, Throwable throwable )
    {
    super( string, throwable );
    }

  /**
   * Constructor TapException creates a new TapException instance.
   *
   * @param throwable of type Throwable
   */
  public TapException( Throwable throwable )
    {
    super( throwable );
    }
  }
