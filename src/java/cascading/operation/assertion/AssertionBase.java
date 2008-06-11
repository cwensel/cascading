/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.assertion;

import cascading.operation.AssertionException;
import cascading.operation.Operation;
import cascading.tuple.Fields;

/**
 *
 */
public abstract class AssertionBase extends Operation
  {
  /** Field message */
  private String message;

  protected AssertionBase()
    {
    super( ANY, Fields.ALL );
    }

  protected AssertionBase( String message )
    {
    this();
    this.message = message;
    }

  protected AssertionBase( int numArgs )
    {
    super( numArgs, Fields.ALL );
    }

  protected AssertionBase( int numArgs, String message )
    {
    this( numArgs );
    this.message = message;
    }

  public String getMessage()
    {
    return message;
    }

  protected void fail()
    {
    throwFail( getMessage() );
    }

  protected void fail( Object... args )
    {
    throwFail( getMessage(), args );
    }

  public static void throwFail( String message )
    {
    throw new AssertionException( message );
    }

  public static void throwFail( String message, Object... args )
    {
    throw new AssertionException( String.format( message, args ) );
    }
  }
