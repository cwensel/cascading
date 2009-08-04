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

package cascading.operation.assertion;

import cascading.operation.AssertionException;
import cascading.operation.AssertionLevel;
import cascading.operation.BaseOperation;
import cascading.operation.PlannedOperation;
import cascading.operation.PlannerLevel;
import cascading.util.Util;

/**
 * Class BaseAssertion is a convenience class for {@link cascading.operation.Assertion} implementations. Subclassing
 * this class is not required, but does provide some conveneince functions for signaling assertion failures.
 *
 * @see cascading.operation.Assertion
 * @see cascading.operation.GroupAssertion
 * @see cascading.operation.ValueAssertion
 */
public abstract class BaseAssertion<C> extends BaseOperation<C> implements PlannedOperation<C>
  {
  /** Field message */
  private String message;

  protected BaseAssertion()
    {
    }

  protected BaseAssertion( String message )
    {
    this.message = message;
    }

  protected BaseAssertion( int numArgs )
    {
    super( numArgs );
    }

  protected BaseAssertion( int numArgs, String message )
    {
    super( numArgs );
    this.message = message;
    }

  public String getMessage()
    {
    return message;
    }

  @Override
  public boolean supportsPlannerLevel( PlannerLevel plannerLevel )
    {
    return plannerLevel instanceof AssertionLevel;
    }

  protected void fail()
    {
    throwFail( Util.formatTrace( this, getMessage() ) );
    }

  protected void fail( Object... args )
    {
    throwFail( Util.formatTrace( this, getMessage() ), args );
    }

  /**
   * Static method throwFail shoudl be used to throw an {@link AssertionException}.
   *
   * @param message of type String, the message to be thrown
   */
  public static void throwFail( String message )
    {
    throw new AssertionException( message );
    }

  /**
   * Static method throwFail shoudl be used to throw an {@link AssertionException}.
   *
   * @param message of type String, the message to be thrown as a format string
   * @param args    of type Object[], the values to be passed into the message format string
   */
  public static void throwFail( String message, Object... args )
    {
    throw new AssertionException( String.format( message, args ) );
    }
  }
