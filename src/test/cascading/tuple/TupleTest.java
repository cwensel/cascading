/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cascading.CascadingTestCase;

/** @version $Id: //depot/calku/cascading/src/test/cascading/tuple/TupleTest.java#2 $ */
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
    }

  public void testCompare3()
    {
    Tuple aTuple = new Tuple( "Just My Luck", "ClaudiaPuig", 3.0, "LisaRose", 3.0 );
    Tuple bTuple = new Tuple( "Just My Luck", "ClaudiaPuig", 3.0, "LisaRose", 3.0 );

    assertEquals( "not equal: aTuple", bTuple, aTuple );
    assertTrue( "not equal than: aTuple = bTuple", aTuple.compareTo( bTuple ) == 0 );

    bTuple = new Tuple( "Just My Luck", "ClaudiaPuig", 3.0, "LisaRose", 2.0 );

    assertTrue( "not less than: aTuple < bTuple", aTuple.compareTo( bTuple ) > 0 );
    }

  public void testCompare4()
    {
    Tuple aTuple = new Tuple( "Just My Luck", "ClaudiaPuig", null, "LisaRose", null );
    Tuple bTuple = new Tuple( "Just My Luck", "ClaudiaPuig", null, "LisaRose", null );

    assertEquals( "not equal: aTuple", bTuple, aTuple );
    assertTrue( "not equal than: aTuple = bTuple", aTuple.compareTo( bTuple ) == 0 );

    bTuple = new Tuple( "Just My Luck", "ClaudiaPuig", null, "Z", null );

    assertTrue( "not less than: aTuple < bTuple", aTuple.compareTo( bTuple ) == "LisaRose".compareTo( "Z" ) );
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
    Tuple aTuple = new Tuple( new TestWritableComparable( "Just My Luck" ), "ClaudiaPuig", "3.0", "LisaRose", "3.0" );

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream( byteArrayOutputStream );

    aTuple.write( dataOutputStream );

    dataOutputStream.flush();

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
    DataInputStream dataInputStream = new DataInputStream( byteArrayInputStream );

    Tuple newTuple = new Tuple();

    newTuple.readFields( dataInputStream );

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
  }
