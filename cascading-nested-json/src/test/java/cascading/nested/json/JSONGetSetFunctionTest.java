/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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

import cascading.CascadingTestCase;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.Test;

import static java.util.Collections.singletonMap;

/**
 *
 */
public class JSONGetSetFunctionTest extends CascadingTestCase
  {
  @Test
  public void testGet() throws Exception
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.nested );

    JSONGetFunction function = new JSONGetFunction( new Fields( "result" ), "/person/name" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "John Doe", ( (TextNode) value ).textValue() );
    }

  @Test
  public void testGetMissing() throws Exception
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.nested );

    JSONGetFunction function = new JSONGetFunction( new Fields( "result" ), "/person/foobar" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNull( value );
    }

  @Test(expected = OperationException.class)
  public void testGetMissingFail() throws Exception
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.nested );

    JSONGetFunction function = new JSONGetFunction( new Fields( "result" ), true, "/person/foobar" );

    invokeFunction( function, entry, new Fields( "result" ) );
    }

  @Test
  public void testGetMap() throws Exception
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.nested );

    // the map ctor really expects large numbers of entries to be useful
    JSONGetFunction function = new JSONGetFunction( singletonMap( new Fields( "result" ), "/person/name" ) );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "John Doe", ( (TextNode) value ).textValue() );
    }

  @Test
  public void testSet() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "name", String.class ) ),
      Tuple.size( 2 )
    );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, "Jane Doe" );

    JSONSetFunction function = new JSONSetFunction( new Fields( "result" ), new Fields( "name", String.class ), "/person/otherName" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/person/otherName" ).textValue() );
    }

  @Test
  public void testSetReplace() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "name", String.class ) ),
      Tuple.size( 2 )
    );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, "Jane Doe" );

    JSONSetFunction function = new JSONSetFunction( new Fields( "result" ), new Fields( "name", String.class ), "/person/name" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/person/name" ).textValue() );
    }

  @Test
  public void testSetDeep() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "name", String.class ) ),
      Tuple.size( 2 )
    );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, "Jane Doe" );

    JSONSetFunction function = new JSONSetFunction( new Fields( "result" ), new Fields( "name", String.class ), "/person/foo/name" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/person/foo/name" ).textValue() );
    }

  @Test
  public void testSetMap() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "name", String.class ) ),
      Tuple.size( 2 )
    );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, "Jane Doe" );

    JSONSetFunction function = new JSONSetFunction( new Fields( "result" ), singletonMap( new Fields( "name", String.class ), "/person/name" ) );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/person/name" ).textValue() );
    }
  }
