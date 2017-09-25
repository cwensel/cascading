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
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

/**
 *
 */
public class JSONBuildAsAggregatorTest extends CascadingTestCase
  {
  @Test
  public void testBuilderAggregator() throws Exception
    {
    Fields fields = new Fields( "result", JSONCoercibleType.TYPE );

    TupleEntry group = new TupleEntry( new Fields( "id", String.class ), new Tuple( "123-45-6789" ) );

    Fields argumentFields = Fields.NONE
      .append( new Fields( "id", String.class ) )
      .append( new Fields( "age", Integer.class ) )
      .append( new Fields( "first", String.class ) )
      .append( new Fields( "last", String.class ) )
      .append( new Fields( "child", String.class ) )
      .append( new Fields( "child-age", String.class ) );

    TupleEntry[] arguments = new TupleEntry[]
      {
        new TupleEntry( argumentFields, new Tuple( "123-45-6789", 22, "john", "doe", "jane", "12" ) ),
        new TupleEntry( argumentFields, new Tuple( "123-45-6789", 22, "john", "doe", "jill", "13" ) ),
        new TupleEntry( argumentFields, new Tuple( "123-45-6789", 22, "john", "doe", "julie", "14" ) )
      };

    BuildSpec spec = new BuildSpec()
      .putInto( "id", "/ssn" )
      .putInto( "age", String.class, "/age" )
      .putInto( "first", "/name/first" )
      .putInto( "last", "/name/last" )
      .addInto( "child", "/children" )
      .addInto( "child-age", Integer.class, "/childAges" );

    JSONBuildAsAggregator aggregator = new JSONBuildAsAggregator( fields, spec );

    TupleListCollector result = invokeAggregator( aggregator, group, arguments, fields );

    ObjectNode value = (ObjectNode) result.iterator().next().getObject( 0 );

    assertNotNull( value );
    assertEquals( "john", value.findPath( "name" ).findPath( "first" ).textValue() );
    assertEquals( JsonNodeType.STRING, value.findPath( "age" ).getNodeType() );
    assertEquals( "22", value.findPath( "age" ).textValue() );
    assertEquals( "123-45-6789", value.findValue( "ssn" ).textValue() );
    assertEquals( JsonNodeType.ARRAY, value.findPath( "childAges" ).getNodeType() );
    assertEquals( 3, value.findPath( "childAges" ).size() );
    assertEquals( JsonNodeType.NUMBER, value.findPath( "childAges" ).get( 0 ).getNodeType() );
    }
  }
