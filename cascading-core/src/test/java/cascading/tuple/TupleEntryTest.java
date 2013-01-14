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

package cascading.tuple;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.TimeZone;

import cascading.CascadingTestCase;
import cascading.tuple.type.CoercibleType;
import cascading.tuple.type.DateType;

public class TupleEntryTest extends CascadingTestCase
  {
  public TupleEntryTest()
    {
    }

  public void testSelect()
    {
    Fields selector = new Fields( "a", "d" );

    TupleEntry entryA = new TupleEntry( new Fields( "a", "b" ), new Tuple( "a", "b" ) );
    TupleEntry entryB = new TupleEntry( new Fields( "c", "d" ), new Tuple( "c", "d" ) );

    Tuple tuple = TupleEntry.select( selector, entryA, entryB );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "a", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "d", tuple.getObject( 1 ) );
    }

  public void testSelect2()
    {
    Fields selector = new Fields( 1, "d" );

    TupleEntry entryA = new TupleEntry( new Fields( "a", "b" ), new Tuple( "a", "b" ) );
    TupleEntry entryB = new TupleEntry( new Fields( "c", "d" ), new Tuple( "c", "d" ) );

    Tuple tuple = TupleEntry.select( selector, entryA, entryB );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "b", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "d", tuple.getObject( 1 ) );
    }

  public void testSelectNotComparable()
    {
    Fields selector = new Fields( 1, "d" );

    Object object = new Object();
    TupleEntry entryA = new TupleEntry( new Fields( "a", "b" ), new Tuple( "a", object ) );
    TupleEntry entryB = new TupleEntry( new Fields( "c", "d" ), new Tuple( "c", "d" ) );

    Tuple tuple = TupleEntry.select( selector, entryA, entryB );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", object, tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "d", tuple.getObject( 1 ) );
    }

  public void testSelectComplex()
    {
    Fields selector = new Fields( -1, -3 );

    Fields fieldsA = new Fields( "a", "b" );
    Fields fieldsB = new Fields( "c", "d" );

    selector = Fields.resolve( selector, fieldsA, fieldsB );

    TupleEntry entryA = new TupleEntry( fieldsA, new Tuple( "a", "b" ) );
    TupleEntry entryB = new TupleEntry( fieldsB, new Tuple( "c", "d" ) );

    Tuple tuple = TupleEntry.select( selector, entryA, entryB );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "d", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "b", tuple.getObject( 1 ) );
    }

  public void testSelectComplex2()
    {
    Fields selector = new Fields( -1, -3 );

    Fields fieldsA = new Fields( "a", 1 );
    Fields fieldsB = new Fields( "c", 1 );

    selector = Fields.resolve( selector, fieldsA, fieldsB );

    TupleEntry entryA = new TupleEntry( fieldsA, new Tuple( "a", "b" ) );
    TupleEntry entryB = new TupleEntry( fieldsB, new Tuple( "c", "d" ) );

    Tuple tuple = TupleEntry.select( selector, entryA, entryB );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "d", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "b", tuple.getObject( 1 ) );
    }

  public void testExtractSet()
    {
    Fields selector = new Fields( 1, "d" );

    TupleEntry entryA = new TupleEntry( new Fields( "a", "b", "c", "d" ), new Tuple( "a", "b", "c", "d" ) );

    Tuple tuple = Tuples.extractTuple( entryA, selector );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "b", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "d", tuple.getObject( 1 ) );

    entryA.setTuple( selector, new Tuple( "B", "D" ) );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "a", entryA.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "B", entryA.getObject( 1 ) );
    assertEquals( "not equal: tuple.get(2)", "c", entryA.getObject( 2 ) );
    assertEquals( "not equal: tuple.get(3)", "D", entryA.getObject( 3 ) );
    }

  public void testExtractSet2()
    {
    Fields selector = new Fields( "d", 1 );

    TupleEntry entryA = new TupleEntry( new Fields( "a", "b", "c", "d" ), new Tuple( "a", "b", "c", "d" ) );

    Tuple tuple = Tuples.extractTuple( entryA, selector );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "d", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "b", tuple.getObject( 1 ) );

    entryA.setTuple( selector, new Tuple( "D", "B" ) );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "a", entryA.getObject( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "B", entryA.getObject( 1 ) );
    assertEquals( "not equal: tuple.get(2)", "c", entryA.getObject( 2 ) );
    assertEquals( "not equal: tuple.get(3)", "D", entryA.getObject( 3 ) );
    }

  public void testUnmodifiable()
    {
    TupleEntry entryA = new TupleEntry( new Fields( "a", "b" ), true );

    Tuple tuple = new Tuple( "a", "b" );

    entryA.setTuple( tuple );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "a", tuple.getObject( 0 ) );

    try
      {
      entryA.set( "a", "A" );
      fail( "did not fail" );
      }
    catch( Exception exception )
      {
      // do nothing
      }

    try
      {
      entryA.getTuple().set( 0, "A" );
      fail( "did not fail" );
      }
    catch( Exception exception )
      {
      // do nothing
      }
    }

  public void testEquals()
    {
    assertEquals(
      new TupleEntry( new Fields( "a", "b" ), new Tuple( "a", "b" ) ),
      new TupleEntry( new Fields( "a", "b" ), new Tuple( "a", "b" ) ) );

    Fields fields = new Fields( "a", "b" );
    fields.setComparator( "b", new StringComparator() );

    assertEquals(
      new TupleEntry( fields, new Tuple( "a", "b" ) ),
      new TupleEntry( new Fields( "a", "b" ), new Tuple( "a", "B" ) ) );

    assertNotSame(
      new TupleEntry( new Fields( "a", "b" ), new Tuple( "a", "B" ) ),
      new TupleEntry( new Fields( "a", "b" ), new Tuple( "a", "b" ) ) );

    assertNotSame(
      new TupleEntry( new Fields( "a", "B" ), new Tuple( "a", "b" ) ),
      new TupleEntry( new Fields( "a", "b" ), new Tuple( "a", "b" ) ) );
    }

  private static class StringComparator implements Comparator<String>, Serializable
    {
    @Override
    public int compare( String lhs, String rhs )
      {
      return lhs.compareToIgnoreCase( rhs );
      }
    }

  public void testCoerceCanonical()
    {
    final SimpleDateFormat dateFormat = new SimpleDateFormat( "dd/MMM/yyyy:HH:mm:ss:SSS Z" );
    Date date = new Date();
    String stringDate = dateFormat.format( date );

    CoercibleType coercible = new DateType( "dd/MMM/yyyy:HH:mm:ss:SSS Z", TimeZone.getDefault() );

    Fields fields = Fields.size( 2 ).applyTypes( coercible, long.class );
    Tuple tuple = new Tuple( date.getTime(), date.getTime() );

    TupleEntry results = new TupleEntry( fields, tuple );

    assertEquals( date.getTime(), results.getObject( 0 ) );
    assertEquals( date.getTime(), results.getLong( 0 ) );
    assertEquals( stringDate, results.getString( 0 ) );

    assertEquals( date.getTime(), results.getObject( 1 ) );
    assertEquals( date.getTime(), results.getLong( 1 ) );
    assertEquals( Long.toString( date.getTime() ), results.getString( 1 ) );
    }

  public void testCoerceOnSet()
    {
    final SimpleDateFormat dateFormat = new SimpleDateFormat( "dd/MMM/yyyy:HH:mm:ss:SSS Z" );
    Date date = new Date();
    String stringDate = dateFormat.format( date );

    CoercibleType coercible = new DateType( "dd/MMM/yyyy:HH:mm:ss:SSS Z", TimeZone.getDefault() );

    Fields fields = Fields.size( 5 ).applyTypes( coercible, coercible, coercible, coercible, long.class );
    Tuple tuple = Tuple.size( 5 );

    TupleEntry results = new TupleEntry( fields, tuple );

    // tests all four elements
    results.setLong( 0, date.getTime() );
    results.setString( 1, stringDate );
    results.setObject( 2, date );
    results.setRaw( 3, date ); // performs no coercion
    results.setString( 4, Long.toString( date.getTime() ) );

    assertTrue( results.getObject( 0 ) instanceof Long );
    assertTrue( results.getObject( 1 ) instanceof Long );
    assertTrue( results.getObject( 2 ) instanceof Long );
    assertTrue( results.getObject( 3 ) instanceof Date );
    assertTrue( results.getObject( 4 ) instanceof Long );

    assertEquals( date.getTime(), results.getObject( 0 ) );
    assertEquals( date.getTime(), results.getLong( 0 ) );
    assertEquals( stringDate, results.getString( 0 ) );

    assertEquals( date.getTime(), results.getObject( 1 ) );
    assertEquals( date.getTime(), results.getLong( 1 ) );
    assertEquals( stringDate, results.getString( 1 ) );

    assertEquals( date.getTime(), results.getObject( 2 ) );
    assertEquals( date.getTime(), results.getLong( 2 ) );
    assertEquals( stringDate, results.getString( 2 ) );

    assertEquals( date, results.getObject( 3 ) );

    assertEquals( date.getTime(), results.getObject( 4 ) );
    assertEquals( date.getTime(), results.getLong( 4 ) );
    assertEquals( (int) date.getTime(), results.getInteger( 4 ) );
    assertEquals( Long.toString( date.getTime() ), results.getString( 4 ) );

    results.setTuple( Tuple.size( 5 ) ); // clear prior results

    Tuple expected = Tuple.size( 5 );
    expected.setLong( 0, date.getTime() );
    expected.setString( 1, stringDate );
    expected.set( 2, date );
    expected.set( 3, date );
    expected.setString( 4, Long.toString( date.getTime() ) );

    results.setCanonicalTuple( expected );

    assertTrue( results.getObject( 0 ) instanceof Long );
    assertTrue( results.getObject( 1 ) instanceof Long );
    assertTrue( results.getObject( 2 ) instanceof Long );
    assertTrue( results.getObject( 3 ) instanceof Long );
    assertTrue( results.getObject( 4 ) instanceof Long );

    assertEquals( date.getTime(), results.getObject( 0 ) );
    assertEquals( date.getTime(), results.getObject( 1 ) );
    assertEquals( date.getTime(), results.getObject( 2 ) );
    assertEquals( date.getTime(), results.getObject( 3 ) );
    assertEquals( date.getTime(), results.getObject( 4 ) );

    }

  public void testCoerceIterable()
    {
    final SimpleDateFormat dateFormat = new SimpleDateFormat( "dd/MMM/yyyy:HH:mm:ss:SSS Z" );

    CoercibleType coercible = new DateType( "dd/MMM/yyyy:HH:mm:ss:SSS Z", TimeZone.getDefault() );

    Date date = new Date();
    String stringDate = dateFormat.format( date );
    Tuple tuple = Tuple.size( 4 );

    Fields fields = Fields.size( 4 ).applyTypes( coercible, coercible, coercible, String.class );
    TupleEntry results = new TupleEntry( fields, tuple );

    results.setObject( 0, date );
    results.setLong( 1, date.getTime() );
    results.setString( 2, stringDate );
    results.setString( 3, stringDate );

    Iterable<String> iterable = results.asIterableOf( String.class );

    int count = 0;
    for( String s : iterable )
      {
      assertEquals( stringDate, s );
      count++;
      }

    assertEquals( count, results.size() );
    }
  }
