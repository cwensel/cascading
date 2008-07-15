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

import cascading.operation.ValueAssertion;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class AssertEquals either asserts the number of constructor values is equal
 * to the number of arguments to the assertion and each value is equal to its corresponding argument.
 */
public class AssertEquals extends AssertionBase implements ValueAssertion
  {
  /** Field values */
  private Tuple values;

  /**
   * Constructor AssertEquals creates a new AssertEquals instance.
   *
   * @param values of type Comparable...
   */
  public AssertEquals( Comparable... values )
    {
    super( values.length, "argument tuple: %s was not equal to values: %s" );

    if( values == null )
      throw new IllegalArgumentException( "values may not be null" );

    if( values.length == 0 )
      throw new IllegalArgumentException( "values may not be empty" );

    this.values = new Tuple( values );
    }

  /** @see cascading.operation.ValueAssertion#doAssert(TupleEntry) */
  public void doAssert( TupleEntry input )
    {
    if( !input.getTuple().equals( values ) )
      fail( input.getTuple().print(), values.print() );
    }

  }