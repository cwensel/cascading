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

import cascading.CascadingTestCase;

/** @version $Id: //depot/calku/cascading/src/test/cascading/tuple/TupleFieldsTest.java#2 $ */
public class TupleFieldsTest extends CascadingTestCase
  {
  private Fields fields;
  private Tuple tuple;

  public TupleFieldsTest()
    {
    super( "tuple fields tests" );
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

    fields = new Fields( "one", "two", "three", "four", "five" );
    }

  public void testHas()
    {
    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.get( fields, new Fields( "one" ) ).get( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "b", tuple.get( fields, new Fields( "two" ) ).get( 0 ) );
    }

  public void testGet()
    {
    Fields aFields = new Fields( "one" );
    Tuple aTuple = tuple.get( fields, aFields );

    assertEquals( "not equal: aTuple.size()", 1, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.get( 0 ) );

    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "b", tuple.get( 1 ) );
    }

  public void testWildcard()
    {
    Fields aFields = Fields.ALL;
    Tuple aTuple = tuple.get( fields, aFields );

    assertEquals( "not equal: aTuple.size()", 5, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.get( fields, new Fields( "one" ) ).get( 0 ) );
    assertEquals( "not equal: aTuple.get( 1 )", "b", aTuple.get( fields, new Fields( "two" ) ).get( 0 ) );
    }

  public void testRemove()
    {
    Fields aFields = new Fields( "one" );
    Tuple aTuple = tuple.remove( fields, aFields );
    assertEquals( "not equal: aTuple.size()", 1, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.get( 0 ) );

    fields = fields.minus( aFields );

    assertEquals( "not equal: tuple.size()", 4, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "b", tuple.get( fields, new Fields( "two" ) ).get( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "c", tuple.get( fields, new Fields( "three" ) ).get( 0 ) );
    }

  public void testPut()
    {
    Fields aFields = new Fields( "one", "five" );
    tuple.put( fields, aFields, new Tuple( "ten", "eleven" ) );

    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "ten", tuple.get( 0 ) );
    assertEquals( "not equal: tuple.get( 0 )", "ten", tuple.get( fields, new Fields( "one" ) ).get( 0 ) );
    assertEquals( "not equal: tuple.get( 0 )", "eleven", tuple.get( 4 ) );
    assertEquals( "not equal: tuple.get( 0 )", "eleven", tuple.get( fields, new Fields( "five" ) ).get( 0 ) );
    }

  public void testSelectComplex()
    {
    Tuple tuple = new Tuple( "movie", "name1", "movie1", "rate1", "name2", "movie2", "rate2" );
    Fields declarationA = new Fields( "movie", "name1", "movie1", "rate1", "name2", "movie2", "rate2" );
    Fields selectA = new Fields( "movie", "name1", "rate1", "name2", "rate2" );

    Tuple result = tuple.get( declarationA, selectA );

    assertEquals( "not equal: ", 5, result.size() );
    assertEquals( "not equal: ", "movie", result.get( 0 ) );
    assertEquals( "not equal: ", "name1", result.get( 1 ) );
    assertEquals( "not equal: ", "rate1", result.get( 2 ) );
    assertEquals( "not equal: ", "name2", result.get( 3 ) );
    assertEquals( "not equal: ", "rate2", result.get( 4 ) );
    }

  }
