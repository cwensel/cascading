/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.tuple;

import cascading.CascadingTestCase;
import cascading.tuple.io.TuplePair;
import org.junit.Test;

public class TupleTest extends CascadingTestCase
  {
  private Tuple tuple;

  public TupleTest()
    {
    }

  @Override
  public void setUp() throws Exception
    {
    super.setUp();

    tuple = new Tuple();

    tuple.add( "a" );
    tuple.add( "b" );
    tuple.add( "c" );
    tuple.add( "d" );
    tuple.add( "d" );
    }

  @Test
  public void testHas()
    {
    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "b", tuple.getObject( 1 ) );
    }

  @Test
  public void testGet()
    {
    Tuple aTuple = tuple.get( new int[]{0} );
    assertEquals( "not equal: aTuple.size()", 1, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.getObject( 0 ) );

    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "b", tuple.getObject( 1 ) );
    }

  @Test
  public void testGetNull()
    {
    Tuple aTuple = tuple.get( (int[]) null );
    assertEquals( "not equal: aTuple.size()", 5, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.getObject( 0 ) );
    assertEquals( "not equal: aTuple.get( 1 )", "b", aTuple.getObject( 1 ) );

    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "b", tuple.getObject( 1 ) );
    }

  @Test
  public void testRemove()
    {
    Tuple aTuple = tuple.remove( new int[]{0} );
    assertEquals( "not equal: aTuple.size()", 1, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.getObject( 0 ) );

    assertEquals( "not equal: tuple.size()", 4, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "b", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "c", tuple.getObject( 1 ) );
    }

  @Test
  public void testRemove2()
    {
    Tuple aTuple = tuple.remove( new int[]{2, 4} );
    assertEquals( "not equal: aTuple.size()", 2, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "c", aTuple.getObject( 0 ) );

    assertEquals( "not equal: tuple.size()", 3, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "b", tuple.getObject( 1 ) );
    assertEquals( "not equal: tuple.get( 1 )", "d", tuple.getObject( 2 ) );
    }

  @Test
  public void testLeave()
    {
    Tuple aTuple = tuple.leave( new int[]{0} );

    assertEquals( "not equal: tuple.size()", 1, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.getObject( 0 ) );

    assertEquals( "not equal: aTuple.size()", 4, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "b", aTuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "c", aTuple.getObject( 1 ) );
    }

  @Test
  public void testExtractSet()
    {
    Tuple aTuple = tuple.extract( new int[]{0} );

    assertEquals( "not equal: aTuple.size()", 1, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.getObject( 0 ) );

    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", null, tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 0 )", "b", tuple.getObject( 1 ) );
    assertEquals( "not equal: tuple.get( 1 )", "c", tuple.getObject( 2 ) );

    tuple.set( new int[]{0}, new Tuple( "A" ) );

    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "A", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 0 )", "b", tuple.getObject( 1 ) );
    assertEquals( "not equal: tuple.get( 1 )", "c", tuple.getObject( 2 ) );
    }

  @Test
  public void testEqualPrimitive()
    {
    assertEquals( "not equal: tuple", new Tuple( 1 ), new Tuple( 1 ) );
    assertEquals( "not equal: tuple", new Tuple( "1" ), new Tuple( "1" ) );
    assertEquals( "not equal: tuple", new Tuple( 1, 2 ), new Tuple( 1, 2 ) );
    assertEquals( "not equal: tuple", new Tuple( "1", 2 ), new Tuple( "1", 2 ) );
    }

  @Test
  public void testEqual()
    {
    Tuple aTuple = new Tuple( tuple );

    assertEquals( "not equal: tuple", aTuple, tuple );

    aTuple.remove( new int[]{0} );

    assertNotSame( "equal: tuple", aTuple, tuple );

    aTuple = new Tuple( tuple, "a" );
    Tuple bTuple = new Tuple( tuple, "a" );

    assertEquals( "not equal: tuple", aTuple, bTuple );

    aTuple = new Tuple( "a", new Tuple( tuple ), "a" ); // test new instances
    bTuple = new Tuple( "a", new Tuple( tuple ), "a" );

    assertEquals( "not equal: tuple", aTuple, bTuple );
    assertEquals( "not equal: hash code", aTuple.hashCode(), bTuple.hashCode() );
    }

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
  public void testCompareNull()
    {
    Tuple aTuple = new Tuple( "a", null, "c" );
    Tuple bTuple = new Tuple( "a", "b", null );

    assertTrue( "not less than: aTuple < bTuple", aTuple.compareTo( bTuple ) < 0 );
    assertTrue( "not less than: bTuple < aTuple", bTuple.compareTo( aTuple ) > 0 );
    }

  @Test
  public void testSetAll()
    {
    Tuple aTuple = new Tuple( tuple );

    int size = aTuple.size() + tuple.size();
    Tuple result = Tuple.size( size );

    result.setAll( aTuple, tuple );

    assertEquals( "wrong size", size, result.size() );

    result.setAll( aTuple, tuple );

    assertEquals( "wrong size", size, result.size() );

    int count = 0;
    for( int i = 0; i < aTuple.size(); i++ )
      assertEquals( "wrong value on: " + count, aTuple.getObject( i ), result.getObject( count++ ) );

    for( int i = 0; i < tuple.size(); i++ )
      assertEquals( "wrong value on: " + count, tuple.getObject( i ), result.getObject( count++ ) );
    }

  public void testSetCoerce()
    {
    Fields fieldsA = new Fields( "a", "b", "c" ).applyTypes( String.class, String.class, String.class );
    Tuple tupleA = new Tuple( "0", "1", "2" );

    Fields fieldsB = new Fields( "c", "b" ).applyTypes( Integer.class, Integer.class );
    Tuple tupleB = new Tuple( -2, -1 );

    tupleA.set( fieldsA, fieldsB, tupleB );

    assertEquals( "wrong size", 3, tupleA.size() );
    assertEquals( "not equal: tuple.get(0)", "0", tupleA.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "-1", tupleA.getObject( 1 ) );
    assertEquals( "not equal: tuple.get(2)", "-2", tupleA.getObject( 2 ) );
    }
  }
