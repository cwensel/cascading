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

package cascading.operation.assertion;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.ValueAssertion;
import cascading.operation.ValueAssertionCall;
import cascading.tuple.TupleEntry;

/**
 * Class AssertSizeLessThan asserts that the current {@link cascading.tuple.Tuple} in the stream has a size less than (&lt;) the given size.
 * </p>
 * On evaluation, {@link cascading.tuple.Tuple#size()} is called (note Tuples may hold {@code null} values).
 */
public class AssertSizeLessThan extends BaseAssertion implements ValueAssertion
  {
  /** Field size */
  private final int size;

  /**
   * Constructor AssertSizeLessThan creates a new AssertSizeLessThan instance.
   *
   * @param size of type int
   */
  @ConstructorProperties({"size"})
  public AssertSizeLessThan( int size )
    {
    super( "tuple size %s, is more than or equal to: %s, in tuple: %s" );
    this.size = size;
    }

  /** @see cascading.operation.ValueAssertion#doAssert(cascading.flow.FlowProcess, cascading.operation.ValueAssertionCall) */
  public void doAssert( FlowProcess flowProcess, ValueAssertionCall assertionCall )
    {
    TupleEntry input = assertionCall.getArguments();

    if( input.size() >= size )
      fail( input.size(), size, input.getTuple().print() );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof AssertSizeLessThan ) )
      return false;
    if( !super.equals( object ) )
      return false;

    AssertSizeLessThan that = (AssertSizeLessThan) object;

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