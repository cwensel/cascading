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

package cascading.operation.filter;

import cascading.CascadingTestCase;
import cascading.operation.Filter;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class FilterTest extends CascadingTestCase
  {
  public FilterTest()
    {
    super( "filter test" );
    }

  private TupleEntry getEntry( Tuple tuple )
    {
    return new TupleEntry( Fields.size( tuple.size() ), tuple );
    }

  public void testNotNull()
    {
    Filter filter = new FilterNotNull();

    assertTrue( filter.isRemove( getEntry( new Tuple( 1 ) ) ) );
    assertFalse( filter.isRemove( getEntry( new Tuple( (Comparable) null ) ) ) );

    assertTrue( filter.isRemove( getEntry( new Tuple( "0", 1 ) ) ) );
    assertTrue( filter.isRemove( getEntry( new Tuple( "0", null ) ) ) );
    assertFalse( filter.isRemove( getEntry( new Tuple( null, null ) ) ) );
    }

  public void testNull()
    {
    Filter filter = new FilterNull();

    assertFalse( filter.isRemove( getEntry( new Tuple( 1 ) ) ) );
    assertTrue( filter.isRemove( getEntry( new Tuple( (Comparable) null ) ) ) );

    assertFalse( filter.isRemove( getEntry( new Tuple( "0", 1 ) ) ) );
    assertTrue( filter.isRemove( getEntry( new Tuple( "0", null ) ) ) );
    assertTrue( filter.isRemove( getEntry( new Tuple( null, null ) ) ) );
    }

  }