/*
 * Copyright (c) 2016-2021 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.nested.json;

import java.util.Iterator;

import cascading.CascadingTestCase;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.Test;

/**
 *
 */
public class JSONGetAllFunctionTest extends CascadingTestCase
  {
  @Test
  public void testGetAll()
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.people );

    JSONGetAllFunction function = new JSONGetAllFunction( "/people/*", new Fields( "result" ), "/person/name" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    assertEquals( 2, result.size() );

    Iterator<Tuple> iterator = result.iterator();
    Object value = iterator.next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "John Doe", ( (TextNode) value ).textValue() );

    value = iterator.next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", ( (TextNode) value ).textValue() );
    }

  @Test
  public void testGetAllCoerced()
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.people );

    JSONGetAllFunction function = new JSONGetAllFunction( "/people/*", new Fields( "result", String.class ), "/person/name" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    assertEquals( 2, result.size() );

    Iterator<Tuple> iterator = result.iterator();
    Object value = iterator.next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "John Doe", value );

    value = iterator.next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value );
    }

  @Test
  public void testGetMissingPointer()
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.people );

    JSONGetAllFunction function = new JSONGetAllFunction( "/notpeople/*", new Fields( "result" ), "/person/foobar" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    assertEquals( 0, result.size() );
    }

  @Test
  public void testGetMissing()
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.people );

    JSONGetAllFunction function = new JSONGetAllFunction( "/people/*", new Fields( "result" ), "/person/foobar" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNull( value );
    }

  @Test(expected = OperationException.class)
  public void testGetMissingFail()
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.people );

    JSONGetFunction function = new JSONGetFunction( new Fields( "result" ), true, "/person/foobar" );

    invokeFunction( function, entry, new Fields( "result" ) );
    }

  @Test
  public void testGetAllDescentCoerced()
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.people );

    JSONGetAllFunction function = new JSONGetAllFunction( "/people/**/name", new Fields( "result", String.class ), "" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    assertEquals( 2, result.size() );

    Iterator<Tuple> iterator = result.iterator();
    Object value = iterator.next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "John Doe", value );

    value = iterator.next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value );
    }
  }
