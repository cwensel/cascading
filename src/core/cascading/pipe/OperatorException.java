/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe;

import cascading.CascadingException;
import cascading.util.Util;

/** Class OperatorException is thrown during field name resolution during planning */
public class OperatorException extends CascadingException
  {
  /** @see cascading.CascadingException#CascadingException() */
  public OperatorException()
    {
    }

  /**
   * Constructor OperatorException creates a new OperatorException instance.
   *
   * @param pipe of type Pipe
   * @param string of type String
   */
  public OperatorException( Pipe pipe, String string )
    {
    super( Util.formatTrace( pipe, string ) );
    }

  /**
   * Constructor OperatorException creates a new OperatorException instance.
   *
   * @param pipe of type Pipe
   * @param string of type String
   * @param throwable of type Throwable
   */
  public OperatorException( Pipe pipe, String string, Throwable throwable )
    {
    super( Util.formatTrace( pipe, string ), throwable );
    }

  /** @see cascading.CascadingException#CascadingException(String) */
  protected OperatorException( String string )
    {
    super( string );
    }

  /** @see cascading.CascadingException#CascadingException(String, Throwable) */
  protected OperatorException( String string, Throwable throwable )
    {
    super( string, throwable );
    }

  /** @see cascading.CascadingException#CascadingException(Throwable) */
  protected OperatorException( Throwable throwable )
    {
    super( throwable );
    }
  }
