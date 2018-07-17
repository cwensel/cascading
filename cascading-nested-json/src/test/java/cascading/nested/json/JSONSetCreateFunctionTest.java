/*
 * Copyright (c) 2016-2018 Chris K Wensel. All Rights Reserved.
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

import java.util.Objects;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import static java.util.Collections.singletonMap;

/**
 *
 */
public class JSONSetCreateFunctionTest extends CascadingTestCase
  {
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

  @Test
  public void testSetMapResolved() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "name", String.class ) ),
      Tuple.size( 2 )
    );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, "Jane Doe" );

    JSONSetFunction function = new JSONSetFunction( new Fields( "result" ), singletonMap( new Fields( 1, String.class ), "/person/name" ) );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/person/name" ).textValue() );
    }

  @Test
  public void testSetResolvedArguments() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "name", String.class ) ),
      Tuple.size( 2 )
    );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, "Jane Doe" );

    Fields fieldDeclaration = new Fields( "result" );
    JSONSetFunction function = new JSONSetFunction( fieldDeclaration );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/name" ).textValue() );
    }

  @Test
  public void testSetResolvedArgumentsWithRoot() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "name", String.class ) ),
      Tuple.size( 2 )
    );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, "Jane Doe" );

    Fields fieldDeclaration = new Fields( "result" );
    JSONSetFunction function = new JSONSetFunction( fieldDeclaration, "/person2" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/person2/name" ).textValue() );
    }

  @Test
  public void testCreate() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "name", String.class ),
      Tuple.size( 1 )
    );

    entry.setObject( 0, "Jane Doe" );

    JSONCreateFunction function = new JSONCreateFunction( new Fields( "result" ), new Fields( "name", String.class ), "/person/otherName" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/person/otherName" ).textValue() );
    }

  @Test
  public void testCreateDeep() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "name", String.class ),
      Tuple.size( 1 )
    );

    entry.setObject( 0, "Jane Doe" );

    JSONCreateFunction function = new JSONCreateFunction( new Fields( "result" ), new Fields( "name", String.class ), "/person/foo/name" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/person/foo/name" ).textValue() );
    }

  @Test
  public void testCreateMap() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "name", String.class ),
      Tuple.size( 1 )
    );

    entry.setObject( 0, "Jane Doe" );

    JSONCreateFunction function = new JSONCreateFunction( new Fields( "result" ), singletonMap( new Fields( "name", String.class ), "/person/name" ) );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/person/name" ).textValue() );
    }

  @Test
  public void testCreateMapResolved() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "name", String.class ),
      Tuple.size( 1 )
    );

    entry.setObject( 0, "Jane Doe" );

    JSONCreateFunction function = new JSONCreateFunction( new Fields( "result" ), singletonMap( new Fields( 0, String.class ), "/person/name" ) );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/person/name" ).textValue() );
    }

  @Test
  public void testCreateResolvedArguments() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "name", String.class ),
      Tuple.size( 1 )
    );

    entry.setObject( 0, "Jane Doe" );

    Fields fieldDeclaration = new Fields( "result" );
    JSONCreateFunction function = new JSONCreateFunction( fieldDeclaration );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/name" ).textValue() );
    }

  @Test
  public void testCreateResolvedArgumentsWithRoot() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "name", String.class ),
      Tuple.size( 1 )
    );

    entry.setObject( 0, "Jane Doe" );

    Fields fieldDeclaration = new Fields( "result" );
    JSONCreateFunction function = new JSONCreateFunction( fieldDeclaration, "/person" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode value = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "Jane Doe", value.at( "/person/name" ).textValue() );
    }

  @Test
  public void testCreatePredicate() throws Exception
    {
    TupleEntry entry = new TupleEntry(
      new Fields( "first", String.class ).append( new Fields( "last", String.class ) ),
      Tuple.size( 2 )
    );

    entry.setObject( 0, "John" );
    entry.setObject( 1, null );

    JSONCreateFunction function = new JSONCreateFunction( new Fields( "result" ), Objects::nonNull );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    JsonNode node = (JsonNode) result.iterator().next().getObject( 0 );

    assertNotNull( node );
    assertEquals( "John", node.at( "/first" ).textValue() );
    assertFalse( node.has( "/last" ) );
    }
  }
