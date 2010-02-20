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
import cascading.tuple.TupleEntry;

/**
 * Class AssertSizeEquals asserts that the current {@link cascading.tuple.Tuple} in the stream is exactly the given size.
 * </p>
 * On evaluation, {@link cascading.tuple.Tuple#size()} is called (note Tuples may hold {@code null} values).
 */
public class AssertSizeEquals extends BaseAssertion implements ValueAssertion
  {
  /** Field size */
  private final int size;

  /**
   * Constructor AssertSizeEquals creates a new AssertSizeEquals instance.
   *
   * @param size of type int
   */
  @ConstructorProperties({"size"})
  public AssertSizeEquals( int size )
    {
    super( "tuple size %s, is not equal to: %s, in tuple: %s" );
    this.size = size;
    }

  /** @see cascading.operation.ValueAssertion#doAssert(cascading.flow.FlowProcess,cascading.operation.ValueAssertionCall) */
  public void doAssert( FlowProcess flowProcess, ValueAssertionCall assertionCall )
    {
    TupleEntry input = assertionCall.getArguments();

    if( input.size() != size )
      fail( input.size(), size, input.getTuple().print() );
    }
  }