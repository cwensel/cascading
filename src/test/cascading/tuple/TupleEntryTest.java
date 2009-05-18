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

import cascading.CascadingTestCase;

/** @version : IntelliJGuide,v 1.13 2001/03/22 22:35:22 SYSTEM Exp $ */
public class TupleEntryTest extends CascadingTestCase
  {
  public TupleEntryTest()
    {
    super( "tuple entry tests" );
    }

  public void testSelect()
    {
    Fields selector = new Fields( "a", "d" );

    TupleEntry entryA = new TupleEntry( new Fields( "a", "b" ), new Tuple( "a", "b" ) );
    TupleEntry entryB = new TupleEntry( new Fields( "c", "d" ), new Tuple( "c", "d" ) );

    Tuple tuple = TupleEntry.select( selector, entryA, entryB );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "a", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "d", tuple.get( 1 ) );
    }

  public void testSelect2()
    {
    Fields selector = new Fields( 1, "d" );

    TupleEntry entryA = new TupleEntry( new Fields( "a", "b" ), new Tuple( "a", "b" ) );
    TupleEntry entryB = new TupleEntry( new Fields( "c", "d" ), new Tuple( "c", "d" ) );

    Tuple tuple = TupleEntry.select( selector, entryA, entryB );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "b", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "d", tuple.get( 1 ) );
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
    assertEquals( "not equal: tuple.get(0)", "d", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "b", tuple.get( 1 ) );
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
    assertEquals( "not equal: tuple.get(0)", "d", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "b", tuple.get( 1 ) );
    }

  public void testExtractSet()
    {
    Fields selector = new Fields( 1, "d" );

    TupleEntry entryA = new TupleEntry( new Fields( "a", "b", "c", "d" ), new Tuple( "a", "b", "c", "d" ) );

    Tuple tuple = Tuples.extractTuple( entryA, selector );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "b", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "d", tuple.get( 1 ) );

    entryA.setTuple( selector, new Tuple( "B", "D" ) );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "a", entryA.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "B", entryA.get( 1 ) );
    assertEquals( "not equal: tuple.get(2)", "c", entryA.get( 2 ) );
    assertEquals( "not equal: tuple.get(3)", "D", entryA.get( 3 ) );
    }

  public void testExtractSet2()
    {
    Fields selector = new Fields( "d", 1 );

    TupleEntry entryA = new TupleEntry( new Fields( "a", "b", "c", "d" ), new Tuple( "a", "b", "c", "d" ) );

    Tuple tuple = Tuples.extractTuple( entryA, selector );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "d", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "b", tuple.get( 1 ) );

    entryA.setTuple( selector, new Tuple( "D", "B" ) );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "a", entryA.get( 0 ) );
    assertEquals( "not equal: tuple.get(1)", "B", entryA.get( 1 ) );
    assertEquals( "not equal: tuple.get(2)", "c", entryA.get( 2 ) );
    assertEquals( "not equal: tuple.get(3)", "D", entryA.get( 3 ) );
    }

  public void testUnmodifiable()
    {
    TupleEntry entryA = new TupleEntry( new Fields( "a", "b" ), true );

    Tuple tuple = new Tuple( "a", "b" );

    entryA.setTuple( tuple );

    assertEquals( "wrong size", 2, tuple.size() );
    assertEquals( "not equal: tuple.get(0)", "a", tuple.get( 0 ) );

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
  }
