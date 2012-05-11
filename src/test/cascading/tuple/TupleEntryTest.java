/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

public class TupleEntryTest extends CascadingTestCase
  {
  public TupleEntryTest()
    {
    super();
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
  }
