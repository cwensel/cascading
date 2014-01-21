/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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
import org.junit.Test;

public class TupleFieldsTest extends CascadingTestCase
  {
  private Fields fields;
  private Tuple tuple;

  public TupleFieldsTest()
    {
    }

  @Override
  public void setUp() throws Exception
    {
    tuple = new Tuple();

    tuple.add( "a" );
    tuple.add( "b" );
    tuple.add( "c" );
    tuple.add( "d" );
    tuple.add( "d" );

    fields = new Fields( "one", "two", "three", "four", "five" );
    }

  @Test
  public void testHas()
    {
    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.get( fields, new Fields( "one" ) ).getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "b", tuple.get( fields, new Fields( "two" ) ).getObject( 0 ) );
    }

  @Test
  public void testGet()
    {
    Fields aFields = new Fields( "one" );
    Tuple aTuple = tuple.get( fields, aFields );

    assertEquals( "not equal: aTuple.size()", 1, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.getObject( 0 ) );

    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "a", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "b", tuple.getObject( 1 ) );
    }

  @Test
  public void testWildcard()
    {
    Fields aFields = Fields.ALL;
    Tuple aTuple = tuple.get( fields, aFields );

    assertEquals( "not equal: aTuple.size()", 5, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.get( fields, new Fields( "one" ) ).getObject( 0 ) );
    assertEquals( "not equal: aTuple.get( 1 )", "b", aTuple.get( fields, new Fields( "two" ) ).getObject( 0 ) );
    }

  @Test
  public void testRemove()
    {
    Fields aFields = new Fields( "one" );
    Tuple aTuple = tuple.remove( fields, aFields );
    assertEquals( "not equal: aTuple.size()", 1, aTuple.size() );
    assertEquals( "not equal: aTuple.get( 0 )", "a", aTuple.getObject( 0 ) );

    fields = fields.subtract( aFields );

    assertEquals( "not equal: tuple.size()", 4, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "b", tuple.get( fields, new Fields( "two" ) ).getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 1 )", "c", tuple.get( fields, new Fields( "three" ) ).getObject( 0 ) );
    }

  @Test
  public void testPut()
    {
    Fields aFields = new Fields( "one", "five" );
    tuple.put( fields, aFields, new Tuple( "ten", "eleven" ) );

    assertEquals( "not equal: tuple.size()", 5, tuple.size() );
    assertEquals( "not equal: tuple.get( 0 )", "ten", tuple.getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 0 )", "ten", tuple.get( fields, new Fields( "one" ) ).getObject( 0 ) );
    assertEquals( "not equal: tuple.get( 0 )", "eleven", tuple.getObject( 4 ) );
    assertEquals( "not equal: tuple.get( 0 )", "eleven", tuple.get( fields, new Fields( "five" ) ).getObject( 0 ) );
    }

  @Test
  public void testSelectComplex()
    {
    Tuple tuple = new Tuple( "movie", "name1", "movie1", "rate1", "name2", "movie2", "rate2" );
    Fields declarationA = new Fields( "movie", "name1", "movie1", "rate1", "name2", "movie2", "rate2" );
    Fields selectA = new Fields( "movie", "name1", "rate1", "name2", "rate2" );

    Tuple result = tuple.get( declarationA, selectA );

    assertEquals( "not equal: ", 5, result.size() );
    assertEquals( "not equal: ", "movie", result.getObject( 0 ) );
    assertEquals( "not equal: ", "name1", result.getObject( 1 ) );
    assertEquals( "not equal: ", "rate1", result.getObject( 2 ) );
    assertEquals( "not equal: ", "name2", result.getObject( 3 ) );
    assertEquals( "not equal: ", "rate2", result.getObject( 4 ) );
    }
  }
