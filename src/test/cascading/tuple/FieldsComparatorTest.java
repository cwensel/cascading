/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple;

import java.util.Comparator;

import cascading.CascadingTestCase;
import cascading.TestStringComparator;

/**
 *
 */
public class FieldsComparatorTest extends CascadingTestCase
  {
  Comparator comparator = new TestStringComparator( false );

  public FieldsComparatorTest()
    {
    super( "fields comparator test" );
    }

  public void testCompare()
    {
    FieldsComparator fields = new FieldsComparator( "a" );

    fields.setComparator( "a", comparator );

    Tuple aTuple = new Tuple( "a" );
    Tuple bTuple = new Tuple( "b" );

    assertTrue( "not less than: aTuple < bTuple", fields.compare( aTuple, bTuple ) < 0 );
    assertTrue( "not less than: bTuple < aTuple", fields.compare( bTuple, aTuple ) > 0 );

    aTuple.add( "b" );

    assertTrue( "not greater than: aTuple > bTuple", fields.compare( aTuple, bTuple ) > 0 );

    aTuple = new Tuple( bTuple, "a" );

    assertTrue( "not greater than: aTuple > bTuple", fields.compare( aTuple, bTuple ) > 0 );
    }

  public void testCompare2()
    {
    FieldsComparator fields = new FieldsComparator( "a" );

    fields.setComparators( new Comparator[]{comparator} );

    Tuple aTuple = new Tuple( "a" );
    Tuple bTuple = new Tuple( "b" );

    assertTrue( "not less than: aTuple < bTuple", fields.compare( aTuple, bTuple ) < 0 );
    assertTrue( "not less than: bTuple < aTuple", fields.compare( bTuple, aTuple ) > 0 );

    aTuple.add( "b" );

    assertTrue( "not greater than: aTuple > bTuple", fields.compare( aTuple, bTuple ) > 0 );

    aTuple = new Tuple( bTuple, "a" );

    assertTrue( "not greater than: aTuple > bTuple", fields.compare( aTuple, bTuple ) > 0 );
    }
  }
