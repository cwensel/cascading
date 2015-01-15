/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.assertion;

import java.beans.ConstructorProperties;

import cascading.operation.AssertionException;
import cascading.operation.AssertionLevel;
import cascading.operation.BaseOperation;
import cascading.operation.PlannedOperation;
import cascading.operation.PlannerLevel;
import cascading.util.TraceUtil;

/**
 * Class BaseAssertion is a convenience class for {@link cascading.operation.Assertion} implementations. Subclassing
 * this class is not required, but does provide some convenience functions for signaling assertion failures.
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

  @ConstructorProperties( {"message"} )
  protected BaseAssertion( String message )
    {
    this.message = message;
    }

  @ConstructorProperties( {"numArgs"} )
  protected BaseAssertion( int numArgs )
    {
    super( numArgs );
    }

  @ConstructorProperties( {"numArgs", "message"} )
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
    throwFail( TraceUtil.formatTrace( this, getMessage() ) );
    }

  protected void fail( Object... args )
    {
    throwFail( TraceUtil.formatTrace( this, getMessage() ), args );
    }

  /**
   * Static method throwFail should be used to throw an {@link AssertionException}.
   *
   * @param message of type String, the message to be thrown
   */
  public static void throwFail( String message )
    {
    throw new AssertionException( message );
    }

  /**
   * Static method throwFail should be used to throw an {@link AssertionException}.
   *
   * @param message of type String, the message to be thrown as a format string
   * @param args    of type Object[], the values to be passed into the message format string
   */
  public static void throwFail( String message, Object... args )
    {
    throw new AssertionException( String.format( message, args ) );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof BaseAssertion ) )
      return false;
    if( !super.equals( object ) )
      return false;

    BaseAssertion that = (BaseAssertion) object;

    if( message != null ? !message.equals( that.message ) : that.message != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( message != null ? message.hashCode() : 0 );
    return result;
    }
  }
