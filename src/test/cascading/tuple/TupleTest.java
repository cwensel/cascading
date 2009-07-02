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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;

import cascading.CascadingTestCase;

public class TupleTest extends CascadingTestCase
  {
  private Tuple tuple;

  public TupleTest()
    {
    super( "tuple tests" );
    }


  @Override
  protected void setUp() throws Exception
    {
    super.setUp();

    tuple = new Tuple();

    tuple.add( "a" );
    tuple.add( "b" );
    tuple.add( "c" );
    tuple.add( "d" );
    tuple.add( "d" );
    }

  public void testHas()
    {
    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "b", tuple.get( 1 ) );
    }

  public void testGet()
    {
    Tuple aTuple = tuple.get( new int[]{0} );
    assertEquals( "not equal: aTuple.size()", 1, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.get( 0 ) );

    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "b", tuple.get( 1 ) );
    }

  public void testGetNull()
    {
    Tuple aTuple = tuple.get( (int[]) null );
    assertEquals( "not equal: aTuple.size()", 5, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.get( 0 ) );
    assertEquals( "not equal: aTuple.get( 1 )", "b", aTuple.get( 1 ) );

    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "b", tuple.get( 1 ) );
    }

  public void testRemove()
    {
    Tuple aTuple = tuple.remove( new int[]{0} );
    assertEquals( "not equal: aTuple.size()", 1, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.get( 0 ) );


    assertEquals( "not equal: tuple.size()", 4, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "b", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "c", tuple.get( 1 ) );
    }

  public void testRemove2()
    {
    Tuple aTuple = tuple.remove( new int[]{2, 4} );
    assertEquals( "not equal: aTuple.size()", 2, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "c", aTuple.get( 0 ) );


    assertEquals( "not equal: tuple.size()", 3, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "b", tuple.get( 1 ) );
    assertEquals( "not equal: tuple.get( 1 )", "d", tuple.get( 2 ) );
    }

  public void testLeave()
    {
    Tuple aTuple = tuple.leave( new int[]{0} );

    assertEquals( "not equal: tuple.size()", 1, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.get( 0 ) );

    assertEquals( "not equal: aTuple.size()", 4, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "b", aTuple.get( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "c", aTuple.get( 1 ) );
    }

  public void testExtractSet()
    {
    Tuple aTuple = tuple.extract( new int[]{0} );

    assertEquals( "not equal: aTuple.size()", 1, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.get( 0 ) );

    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", null, tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get( 0 )", "b", tuple.get( 1 ) );
    assertEquals( "not equal: tuple.get( 1 )", "c", tuple.get( 2 ) );

    tuple.set( new int[]{0}, new Tuple( "A" ) );

    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "A", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get( 0 )", "b", tuple.get( 1 ) );
    assertEquals( "not equal: tuple.get( 1 )", "c", tuple.get( 2 ) );
    }

  public void testEqualPrimitive()
    {
    assertEquals( "not equal: tuple", new Tuple( 1 ), new Tuple( 1 ) );
    assertEquals( "not equal: tuple", new Tuple( "1" ), new Tuple( "1" ) );
    assertEquals( "not equal: tuple", new Tuple( 1, 2 ), new Tuple( 1, 2 ) );
    assertEquals( "not equal: tuple", new Tuple( "1", 2 ), new Tuple( "1", 2 ) );
    }

  public void testEqual()
    {
    Tuple aTuple = new Tuple( tuple );

    assertEquals( "not equal: tuple", aTuple, tuple );

    aTuple.remove( new int[]{0} );

    assertNotSame( "equal: tuple", aTuple, tuple );

    aTuple = new Tuple( tuple, "a" );
    Tuple bTuple = new Tuple( tuple, "a" );

    assertEquals( "not equal: tuple", aTuple, bTuple );
    }

  public void testCompare()
    {
    Tuple aTuple = new Tuple( "a" );
    Tuple bTuple = new Tuple( "b" );

    assertTrue( "not less than: aTuple < bTuple", aTuple.compareTo( bTuple ) < 0 );
    assertTrue( "not less than: bTuple < aTuple", bTuple.compareTo( aTuple ) > 0 );

    aTuple.add( "b" );

    assertTrue( "not greater than: aTuple > bTuple", aTuple.compareTo( bTuple ) > 0 );

    aTuple = new Tuple( bTuple, "a" );

    assertTrue( "not greater than: aTuple > bTuple", aTuple.compareTo( bTuple ) > 0 );

    bTuple = new Tuple( bTuple, "b" );

    assertTrue( "not less than: aTuple < bTuple", aTuple.compareTo( bTuple ) < 0 );
    }

  public void testCompare2()
    {
    Tuple aTuple = new Tuple( "Just My Luck", "ClaudiaPuig", "3.0", "LisaRose", "3.0" );
    Tuple bTuple = new Tuple( "Just My Luck", "ClaudiaPuig", "3.0", "LisaRose", "3.0" );

    assertEquals( "not equal: aTuple", bTuple, aTuple );
    assertTrue( "not equal than: aTuple = bTuple", aTuple.compareTo( bTuple ) == 0 );

    bTuple = new Tuple( "Just My Luck", "ClaudiaPuig", "3.0", "LisaRose", "2.0" );

    assertTrue( "not less than: aTuple < bTuple", aTuple.compareTo( bTuple ) > 0 );
    assertTrue( "not less than: bTuple > aTuple", bTuple.compareTo( aTuple ) < 0 );
    }

  public void testCompare3()
    {
    Tuple aTuple = new Tuple( "Just My Luck", "ClaudiaPuig", 3.0, "LisaRose", 3.0 );
    Tuple bTuple = new Tuple( "Just My Luck", "ClaudiaPuig", 3.0, "LisaRose", 3.0 );

    assertEquals( "not equal: aTuple", bTuple, aTuple );
    assertTrue( "not equal than: aTuple = bTuple", aTuple.compareTo( bTuple ) == 0 );

    bTuple = new Tuple( "Just My Luck", "ClaudiaPuig", 3.0, "LisaRose", 2.0 );

    assertTrue( "not less than: aTuple < bTuple", aTuple.compareTo( bTuple ) > 0 );
    assertTrue( "not less than: bTuple > aTuple", bTuple.compareTo( aTuple ) < 0 );
    }

  public void testCompare4()
    {
    Tuple aTuple = new Tuple( "Just My Luck", "ClaudiaPuig", null, "LisaRose", null );
    Tuple bTuple = new Tuple( "Just My Luck", "ClaudiaPuig", null, "LisaRose", null );

    assertEquals( "not equal: aTuple", bTuple, aTuple );
    assertTrue( "not equal than: aTuple = bTuple", aTuple.compareTo( bTuple ) == 0 );

    bTuple = new Tuple( "Just My Luck", "ClaudiaPuig", null, "Z", null );

    assertTrue( "not less than: aTuple < bTuple", aTuple.compareTo( bTuple ) == "LisaRose".compareTo( "Z" ) );
    assertTrue( "not less than: bTuple > aTuple", bTuple.compareTo( aTuple ) == "Z".compareTo( "LisaRose" ) );
    }

  public void testPairCompare()
    {
    TuplePair aTuple = new TuplePair( new Tuple( "Just My Luck", "ClaudiaPuig", 3.0, "LisaRose", 3.0 ), new Tuple( "a" ) );
    TuplePair bTuple = new TuplePair( new Tuple( "Just My Luck", "ClaudiaPuig", 3.0, "LisaRose", 3.0 ), new Tuple( "a" ) );

    assertEquals( "not equal: aTuple", bTuple, aTuple );
    assertTrue( "not equal than: aTuple = bTuple", aTuple.compareTo( bTuple ) == 0 );

    bTuple = new TuplePair( new Tuple( "Just My Luck", "ClaudiaPuig", 2.0, "LisaRose", 3.0 ), new Tuple( "a" ) );

    assertTrue( "not less than: aTuple > bTuple", aTuple.compareTo( bTuple ) > 0 );
    assertTrue( "not less than: bTuple < aTuple", bTuple.compareTo( aTuple ) < 0 );

    bTuple = new TuplePair( new Tuple( "Just My Luck", "ClaudiaPuig", 3.0, "LisaRose", 3.0 ), new Tuple( "b" ) );

    assertTrue( "not less than: aTuple < bTuple", aTuple.compareTo( bTuple ) < 0 );
    assertTrue( "not less than: bTuple > aTuple", bTuple.compareTo( aTuple ) > 0 );
    }

  public void testCompareNull()
    {
    Tuple aTuple = new Tuple( "a", null, "c" );
    Tuple bTuple = new Tuple( "a", "b", null );

    assertTrue( "not less than: aTuple < bTuple", aTuple.compareTo( bTuple ) < 0 );
    assertTrue( "not less than: bTuple < aTuple", bTuple.compareTo( aTuple ) > 0 );
    }


  public void testParse()
    {
    assertTuple( new Tuple( 0 ) );
    assertTuple( new Tuple( 0, 1 ) );
    assertTuple( new Tuple( "value" ) );
    assertTuple( new Tuple( "value", "value" ) );
    assertTuple( new Tuple( "value", "value", "value" ) );
    assertTuple( new Tuple( "value", "value", 0 ) );
    assertTuple( new Tuple( "value", "value", -1 ) );
    assertTuple( new Tuple( "value", "value", 66.9900d ) );
    }

  private void assertTuple( Tuple value )
    {
    assertEquals( "not same tuple: " + value.print(), value, Tuple.parse( value.print() ) );
//    assertEquals( "not same tuple: "+ value.print(), value, Tuple.parse( value.toString() ) );
    }

  public void testWritableCompareReadWrite() throws IOException
    {
    Tuple aTuple = new Tuple( new TestWritableComparable( "Just My Luck" ), "ClaudiaPuig", "3.0", "LisaRose", "3.0", true );

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    TupleOutputStream dataOutputStream = new TupleOutputStream( byteArrayOutputStream );

    dataOutputStream.writeTuple( aTuple );

    dataOutputStream.flush();

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
    TupleInputStream dataInputStream = new TupleInputStream( byteArrayInputStream );

    Tuple newTuple = new Tuple();

    dataInputStream.readTuple( newTuple );

    assertEquals( aTuple, newTuple );
    }

  public void testWritableCompare()
    {
    Tuple aTuple = new Tuple( new TestWritableComparable( "Just My Luck" ), "ClaudiaPuig", "3.0", "LisaRose", "3.0" );
    Tuple bTuple = new Tuple( new TestWritableComparable( "Just My Luck" ), "ClaudiaPuig", "3.0", "LisaRose", "3.0" );

    assertEquals( "not equal: aTuple", bTuple, aTuple );
    assertTrue( "not equal than: aTuple = bTuple", aTuple.compareTo( bTuple ) == 0 );

    bTuple = new Tuple( new TestWritableComparable( "Just My Luck" ), "ClaudiaPuig", "3.0", "LisaRose", "2.0" );

    assertTrue( "not less than: aTuple < bTuple", aTuple.compareTo( bTuple ) > 0 );
    }

  public void testCoerce()
    {
    Date date = new Date();
    Tuple tuple = new Tuple( "1", null, date, date );

    Tuple results = Tuples.coerce( tuple, new Class[]{int.class, boolean.class, Date.class, String.class} );

    assertEquals( results.get( 0 ), 1 );
    assertEquals( results.get( 1 ), false );
    assertEquals( results.get( 2 ), date );
    assertEquals( results.get( 3 ), date.toString() );
    }
  }
