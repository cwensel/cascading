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
 * Class AssertSizeMoreThan asserts that the current {@link cascading.tuple.Tuple} in the stream has a size more than (&gt;) the given size.
 * </p>
 * On evaluation, {@link cascading.tuple.Tuple#size()} is called (note Tuples may hold {@code null} values).
 */
public class AssertSizeMoreThan extends BaseAssertion implements ValueAssertion
  {
  /** Field size */
  private final int size;

  /**
   * Constructor AssertSizeMoreThan creates a new AssertSizeMoreThan instance.
   *
   * @param size of type int
   */
  @ConstructorProperties({"size"})
  public AssertSizeMoreThan( int size )
    {
    super( "tuple size %s, is less than or equal to: %s, in tuple: %s" );
    this.size = size;
    }

  public int getSize()
    {
    return size;
    }

  @Override
  public void doAssert( FlowProcess flowProcess, ValueAssertionCall assertionCall )
    {
    TupleEntry input = assertionCall.getArguments();

    if( input.size() <= size )
      fail( input.size(), size, input.getTuple().print() );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof AssertSizeMoreThan ) )
      return false;
    if( !super.equals( object ) )
      return false;

    AssertSizeMoreThan that = (AssertSizeMoreThan) object;

    if( size != that.size )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + size;
    return result;
    }
  }
