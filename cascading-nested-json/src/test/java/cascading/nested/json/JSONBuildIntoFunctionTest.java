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
import cascading.nested.core.BuildSpec;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

/**
 *
 */
public class JSONBuildIntoFunctionTest extends CascadingTestCase
  {
  @Test
  public void testBuildIntoFunction() throws Exception
    {
    Fields fields = new Fields( "result", JSONCoercibleType.TYPE );

    Fields argumentFields = Fields.NONE
      .append( new Fields( "id", String.class ) )
      .append( new Fields( "age", Integer.class ) )
      .append( new Fields( "first", String.class ) )
      .append( new Fields( "last", String.class ) )
      .append( new Fields( "child", String.class ) )
      .append( new Fields( "child-age", String.class ) )
      .append( new Fields( "result", JSONCoercibleType.TYPE ) );

    TupleEntry arguments = new TupleEntry( argumentFields, Tuple.size( 7 ) );

    arguments.setObject( 0, "123-45-6789" );
    arguments.setObject( 1, 50 );
    arguments.setObject( 2, "John" );
    arguments.setObject( 3, "Doe" );
    arguments.setObject( 4, "Jane" );
    arguments.setObject( 5, 4 );
    arguments.setObject( 6, JsonNodeFactory.instance.objectNode() );

    BuildSpec spec = new BuildSpec()
      .putInto( "id", "/ssn" )
      .putInto( "age", String.class, "/age" )
      .putInto( "first", "/name/first" )
      .putInto( "last", "/name/last" )
      .addInto( "child", "/children" )
      .addInto( "child-age", Integer.class, "/childAges" );

    JSONBuildIntoFunction function = new JSONBuildIntoFunction( fields, spec );

    TupleListCollector result = invokeFunction( function, arguments, fields );

    ObjectNode value = (ObjectNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "John", value.findPath( "name" ).findPath( "first" ).textValue() );
    assertEquals( JsonNodeType.STRING, value.findPath( "age" ).getNodeType() );
    assertEquals( "50", value.findPath( "age" ).textValue() );
    assertEquals( "123-45-6789", value.findValue( "ssn" ).textValue() );
    assertEquals( JsonNodeType.ARRAY, value.findPath( "childAges" ).getNodeType() );
    assertEquals( 1, value.findPath( "childAges" ).size() );
    assertEquals( JsonNodeType.NUMBER, value.findPath( "childAges" ).get( 0 ).getNodeType() );
    }
  }
