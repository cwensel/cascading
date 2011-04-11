/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

/**
 * Class AssertionException is the exception that must be thrown when an {@link cascading.operation.Assertion}
 * fails.
 */
public class AssertionException extends OperationException
  {
  /** @see OperationException#OperationException() */
  public AssertionException()
    {
    }

  /** @see OperationException#OperationException(String) */
  public AssertionException( String string )
    {
    super( string );
    }

  /** @see OperationException#OperationException(String, Throwable) */
  public AssertionException( String string, Throwable throwable )
    {
    super( string, throwable );
    }

  /** @see OperationException#OperationException(Throwable) */
  public AssertionException( Throwable throwable )
    {
    super( throwable );
    }
  }
