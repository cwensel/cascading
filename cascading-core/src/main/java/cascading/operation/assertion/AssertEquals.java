/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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
import java.util.Collection;

import cascading.flow.FlowProcess;
import cascading.management.annotation.Property;
import cascading.management.annotation.PropertyDescription;
import cascading.management.annotation.Visibility;
import cascading.operation.ValueAssertion;
import cascading.operation.ValueAssertionCall;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;

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

  @Property(name = "values", visibility = Visibility.PRIVATE)
  @PropertyDescription("The expected values.")
  public Collection getValues()
    {
    return Tuples.asCollection( values );
    }

  @Override
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