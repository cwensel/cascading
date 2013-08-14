/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.FlowProcess;
import cascading.operation.ValueAssertion;
import cascading.operation.ValueAssertionCall;
import cascading.tuple.TupleEntry;

/**
 * Class AssertEqualsAll asserts that every value in the argument values {@link cascading.tuple.Tuple} is equal to the value
 * provided on the constructor.
 */
public class AssertEqualsAll extends BaseAssertion implements ValueAssertion
  {
  /** Field value */
  private Object value;

  /**
   * Constructor AssertEqualsAll creates a new AssertEqualsAll instance.
   *
   * @param value of type Comparable
   */
  @ConstructorProperties({"value"})
  public AssertEqualsAll( Object value )
    {
    super( "argument '%s' value was: %s, not: %s, in tuple: %s" );

    if( value == null )
      throw new IllegalArgumentException( "value may not be null" );

    this.value = value;
    }

  public Object getValue()
    {
    return value;
    }

  @Override
  public void doAssert( FlowProcess flowProcess, ValueAssertionCall assertionCall )
    {
    TupleEntry input = assertionCall.getArguments();
    int pos = 0;

    for( Object element : input.getTuple() )
      {
      if( !value.equals( element ) )
        fail( input.getFields().get( pos ), element, value, input.getTuple().print() );

      pos++;
      }
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof AssertEqualsAll ) )
      return false;
    if( !super.equals( object ) )
      return false;

    AssertEqualsAll that = (AssertEqualsAll) object;

    if( value != null ? !value.equals( that.value ) : that.value != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( value != null ? value.hashCode() : 0 );
    return result;
    }
  }