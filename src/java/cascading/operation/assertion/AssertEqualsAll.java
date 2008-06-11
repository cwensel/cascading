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
import cascading.tuple.TupleEntry;

/**
 * Class AssertEqualsAll asserts that every value in the argument values {@link cascading.tuple.Tuple} is equal to the value
 * provided on the constructor.
 */
public class AssertEqualsAll extends AssertionBase implements ValueAssertion
  {
  /** Field value */
  private Comparable value;

  /**
   * Constructor AssertEqualsAll creates a new AssertEqualsAll instance.
   *
   * @param value of type Comparable
   */
  public AssertEqualsAll( Comparable value )
    {
    super( "argument '%s' value was: %s, not: %s, in tuple: %s" );

    if( value == null )
      throw new IllegalArgumentException( "value may not be null" );

    this.value = value;
    }

  /** @see cascading.operation.ValueAssertion#doAssert(TupleEntry) */
  public void doAssert( TupleEntry input )
    {
    int pos = 0;

    for( Object element : input.getTuple() )
      {
      if( !value.equals( element ) )
        fail( input.getFields().get( pos ), element, value, input.getTuple().print() );

      pos++;
      }
    }

  }