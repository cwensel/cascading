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

import cascading.operation.Assertion;
import cascading.operation.AssertionException;
import cascading.operation.Operation;
import cascading.tuple.Fields;

/**
 *
 */
public abstract class BaseAssertion extends Operation implements Assertion
  {
  private String message;

  protected BaseAssertion()
    {
    super( ANY, Fields.ALL );
    }

  protected BaseAssertion( String message )
    {
    this();
    this.message = message;
    }

  protected BaseAssertion( int numArgs )
    {
    super( numArgs, Fields.ALL );
    }

  protected BaseAssertion( int numArgs, String message )
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
    fail( getMessage() );
    }

  protected void fail( Object... args )
    {
    fail( getMessage(), args );
    }

  public static void fail( String message )
    {
    throw new AssertionException( message );
    }

  public static void fail( String message, Object... args )
    {
    throw new AssertionException( String.format( message, args ) );
    }
  }
