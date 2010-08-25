/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.ValueAssertion;
import cascading.operation.ValueAssertionCall;
import cascading.tuple.Tuple;

/**
 * Class AssertEquals asserts the number of constructor values is equal
 * to the number of argument values {@link Tuple} and each constructor value is {@code .equals()} to its corresponding argument value.
 */
public class AssertEquals extends BaseAssertion implements ValueAssertion
  {
  /** Field values */
  private Tuple values;

  /**
   * Constructor AssertEquals creates a new AssertEquals instance.
   *
   * @param values of type Object...
   */
  @ConstructorProperties({"values"})
  public AssertEquals( Object... values )
    {
    // set to 1 if null, will fail immediately afterwards
    super( values == null ? 1 : values.length, "argument tuple: %s was not equal to values: %s" );

    if( values == null )
      throw new IllegalArgumentException( "values may not be null" );

    if( values.length == 0 )
      throw new IllegalArgumentException( "values may not be empty" );

    this.values = new Tuple( values );
    }

  /** @see cascading.operation.ValueAssertion#doAssert(cascading.flow.FlowProcess,cascading.operation.ValueAssertionCall) */
  public void doAssert( FlowProcess flowProcess, ValueAssertionCall assertionCall )
    {
    Tuple tuple = assertionCall.getArguments().getTuple();

    if( !tuple.equals( values ) )
      fail( tuple.print(), values.print() );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof AssertEquals ) )
      return false;
    if( !super.equals( object ) )
      return false;

    AssertEquals that = (AssertEquals) object;

    if( values != null ? !values.equals( that.values ) : that.values != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( values != null ? values.hashCode() : 0 );
    return result;
    }
  }