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
import cascading.nested.core.CopySpec;
import cascading.nested.json.filter.JSONBooleanPointerFilter;
import cascading.nested.json.filter.JSONStringPointerFilter;
import cascading.nested.json.transform.JSONPrimitiveTransforms;
import cascading.nested.json.transform.JSONSetTextTransform;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

/**
 *
 */
public class JSONCopyIntoFunctionTest extends CascadingTestCase
  {
  @Test
  public void testCopyInto() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .from( "/person" );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "John Doe", ( (ObjectNode) value ).get( "name" ).textValue() );
    assertEquals( 50, ( (ObjectNode) value ).get( "age" ).intValue() );
    assertEquals( "123-45-6789", ( (ObjectNode) value ).get( "ssn" ).textValue() );
    }

  @Test
  public void testCopyIntoPredicate() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .from( "/person", new JSONStringPointerFilter( "/name", "John Doe" ) );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "John Doe", ( (ObjectNode) value ).get( "name" ).textValue() );
    assertEquals( 50, ( (ObjectNode) value ).get( "age" ).intValue() );
    assertEquals( "123-45-6789", ( (ObjectNode) value ).get( "ssn" ).textValue() );
    }

  @Test
  public void testCopyIntoPredicateNegate() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .from( "/person", new JSONStringPointerFilter( "/name", "John Doe" ).negate() );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertNull( ( (ObjectNode) value ).get( "name" ) );
    assertNull( ( (ObjectNode) value ).get( "age" ) );
    assertNull( ( (ObjectNode) value ).get( "ssn" ) );
    }

  @Test
  public void testCopyIntoPredicateBoolean() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .from( "/person", new JSONBooleanPointerFilter( "/human", true ) );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "John Doe", ( (ObjectNode) value ).get( "name" ).textValue() );
    assertEquals( true, ( (ObjectNode) value ).get( "human" ).booleanValue() );
    assertEquals( 50, ( (ObjectNode) value ).get( "age" ).intValue() );
    assertEquals( "123-45-6789", ( (ObjectNode) value ).get( "ssn" ).textValue() );
    }

  @Test
  public void testCopyIntoPredicateBooleanNegate() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .from( "/person", new JSONBooleanPointerFilter( "/human", true ).negate() );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertNull( ( (ObjectNode) value ).get( "name" ) );
    assertNull( ( (ObjectNode) value ).get( "human" ) );
    assertNull( ( (ObjectNode) value ).get( "age" ) );
    assertNull( ( (ObjectNode) value ).get( "ssn" ) );
    }

  @Test
  public void testCopyIntoArrayPredicate() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.people );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .from( "/people/*", new JSONStringPointerFilter( "/person/name", "John Doe" ) );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "John Doe", ( (ObjectNode) value ).get( "person" ).get( "name" ).textValue() );
    assertEquals( 50, ( (ObjectNode) value ).get( "person" ).get( "age" ).intValue() );
    assertEquals( "123-45-6789", ( (ObjectNode) value ).get( "person" ).get( "ssn" ).textValue() );
    }

  @Test
  public void testCopyIntoArrayPredicateNegate() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.people );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .from( "/people/*", new JSONStringPointerFilter( "/person/name", "John Doe" ).negate() );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "Jane Doe", ( (ObjectNode) value ).get( "person" ).get( "name" ).textValue() );
    assertEquals( 49, ( (ObjectNode) value ).get( "person" ).get( "age" ).intValue() );
    assertEquals( "123-45-6789", ( (ObjectNode) value ).get( "person" ).get( "ssn" ).textValue() );
    }

  @Test
  public void testCopyIntoInto() throws Exception
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.people );

    CopySpec copySpec = new CopySpec( "/people" )
      .from( "/people/0" );

    JSONCopyAsFunction function = new JSONCopyAsFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );

    value = ( (ObjectNode) value ).get( "people" ).get( "person" );

    assertEquals( "John Doe", ( (ObjectNode) value ).get( "name" ).textValue() );
    assertEquals( 50, ( (ObjectNode) value ).get( "age" ).intValue() );
    assertEquals( "123-45-6789", ( (ObjectNode) value ).get( "ssn" ).textValue() );
    }

  @Test
  public void testCopyIncludeFrom() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .fromInclude( "/person", "/firstName" )
      .fromInclude( "/person", "/age" );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "John", ( (ObjectNode) value ).get( "firstName" ).textValue() );
    assertEquals( 50, ( (ObjectNode) value ).get( "age" ).intValue() );
    assertEquals( null, ( (ObjectNode) value ).get( "ssn" ) );
    }

  @Test
  public void testCopyIncludeFromPredicate() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .fromInclude( "/person", "/firstName", new JSONStringPointerFilter( "John" ).negate() )
      .fromInclude( "/person", "/age" );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertNull( ( (ObjectNode) value ).get( "firstName" ) );
    assertEquals( 50, ( (ObjectNode) value ).get( "age" ).intValue() );
    assertEquals( null, ( (ObjectNode) value ).get( "ssn" ) );
    }

  @Test
  public void testCopyInclude() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .include( "/person/firstName" )
      .include( "/person/age" );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "John", ( (ObjectNode) value ).findPath( "firstName" ).textValue() );
    assertEquals( 50, ( (ObjectNode) value ).findPath( "age" ).intValue() );
    assertEquals( null, ( (ObjectNode) value ).findValue( "ssn" ) );
    }

  @Test
  public void testCopyIncludeWild() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .include( "/person/firstName" )
      .include( "/*/age" );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "John", ( (ObjectNode) value ).findPath( "firstName" ).textValue() );
    assertEquals( 50, ( (ObjectNode) value ).findPath( "age" ).intValue() );
    assertEquals( null, ( (ObjectNode) value ).findValue( "ssn" ) );
    }

  @Test
  public void testCopyIncludeDescent() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .include( "/person/firstName" )
      .include( "/**/age" );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "John", ( (ObjectNode) value ).findPath( "firstName" ).textValue() );
    assertEquals( 50, ( (ObjectNode) value ).findPath( "age" ).intValue() );
    assertEquals( null, ( (ObjectNode) value ).findValue( "ssn" ) );
    }

  @Test
  public void testCopyIncludeFrom2() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .fromInclude( "/person", "/firstName", "/age" );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "John", ( (ObjectNode) value ).get( "firstName" ).textValue() );
    assertEquals( 50, ( (ObjectNode) value ).get( "age" ).intValue() );
    assertEquals( null, ( (ObjectNode) value ).get( "ssn" ) );
    }

  @Test
  public void testCopyExcludeFrom() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .fromExclude( "/person", "/ssn", "/children" );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "John Doe", ( (ObjectNode) value ).get( "name" ).textValue() );
    assertEquals( 50, ( (ObjectNode) value ).get( "age" ).intValue() );
    assertEquals( null, ( (ObjectNode) value ).get( "ssn" ) );
    assertEquals( null, ( (ObjectNode) value ).get( "children" ) );
    }

  @Test
  public void testCopyExclude() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .exclude( "/person/ssn", "/person/children" );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "John Doe", ( (ObjectNode) value ).findPath( "name" ).textValue() );
    assertEquals( 50, ( (ObjectNode) value ).findPath( "age" ).intValue() );
    assertEquals( null, ( (ObjectNode) value ).findValue( "ssn" ) );
    assertEquals( null, ( (ObjectNode) value ).findValue( "children" ) );
    }

  @Test
  public void testCopyExcludeDescent() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .exclude( "/**/value" );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertNotNull( ( (ObjectNode) value ).get( "person" ) );

    value = ( (ObjectNode) value ).get( "person" );
    assertNull( ( (ObjectNode) value ).get( "measure" ).get( "value" ) );
    }

  @Test
  public void testCoerce() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .fromTransform( "/person/measure", "/value", JSONPrimitiveTransforms.TO_FLOAT );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( JsonNodeType.NUMBER, ( (ObjectNode) value ).get( "value" ).getNodeType() );
    assertEquals( FloatNode.class, ( (ObjectNode) value ).get( "value" ).getClass() );
    assertEquals( 100.0F, ( (ObjectNode) value ).get( "value" ).floatValue() );
    }

  @Test
  public void testCoerceZero() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .fromTransform( "/person/zero", "/zeroValue", JSONPrimitiveTransforms.TO_FLOAT );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( JsonNodeType.NUMBER, ( (ObjectNode) value ).get( "zeroValue" ).getNodeType() );
    assertEquals( FloatNode.class, ( (ObjectNode) value ).get( "zeroValue" ).getClass() );
    assertEquals( 0.0F, ( (ObjectNode) value ).get( "zeroValue" ).floatValue() );
    assertEquals( "0.0", ( (ObjectNode) value ).get( "zeroValue" ).asText() );
    }

  @Test
  public void testCoerceArray() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE ).append( new Fields( "result", JSONCoercibleType.TYPE ) );
    TupleEntry entry = new TupleEntry( fields, Tuple.size( 2 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 1, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .fromTransform( "/person", "/measures/*/value", JSONPrimitiveTransforms.TO_FLOAT );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( JsonNodeType.ARRAY, ( (ObjectNode) value ).get( "measures" ).getNodeType() );
    assertEquals( FloatNode.class, ( (ObjectNode) value ).get( "measures" ).get( 0 ).get( "value" ).getClass() );
    assertEquals( 1000.0F, ( (ObjectNode) value ).get( "measures" ).get( 0 ).get( "value" ).floatValue() );
    assertEquals( FloatNode.class, ( (ObjectNode) value ).get( "measures" ).get( 1 ).get( "value" ).getClass() );
    assertEquals( 2000.0F, ( (ObjectNode) value ).get( "measures" ).get( 1 ).get( "value" ).floatValue() );
    }

  @Test
  public void testResettableTransform() throws Exception
    {
    Fields fields = new Fields( "json", JSONCoercibleType.TYPE )
      .append( new Fields( "set-text", String.class ) )
      .append( new Fields( "result", JSONCoercibleType.TYPE ) );

    TupleEntry entry = new TupleEntry( fields, Tuple.size( 3 ) );

    entry.setObject( 0, JSONData.nested );
    entry.setObject( 2, JSONData.simple );

    CopySpec copySpec = new CopySpec()
      .fromTransform( "/person", "/name", new JSONSetTextTransform( "set-text", "value1" ) );

    JSONCopyIntoFunction function = new JSONCopyIntoFunction( new Fields( "result" ), copySpec );

    entry.setObject( 1, "value2" );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    Object value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "value2", ( (ObjectNode) value ).get( "name" ).textValue() );

    entry.setObject( 1, "value3" );

    result = invokeFunction( function, entry, new Fields( "result" ) );

    value = result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "value", ( (ObjectNode) value ).get( "existing" ).textValue() ); // confirm we put data into an existing object
    assertEquals( "value3", ( (ObjectNode) value ).get( "name" ).textValue() );
    }
  }

