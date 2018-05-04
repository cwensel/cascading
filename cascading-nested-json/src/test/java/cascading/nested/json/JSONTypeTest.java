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

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class JSONTypeTest
  {
  @Test
  public void stringLiteralCoercions()
    {
    testCoercion( "\"Foo\"", JsonNodeType.STRING, "Foo", String.class );
    testCoercion( "Foo", JsonNodeType.STRING, "Foo", String.class );
    testCoercion( "100", JsonNodeType.NUMBER, 100, Integer.class );
    testCoercion( 100, JsonNodeType.NUMBER, 100, Integer.class );
    }

  private void testCoercion( Object value, JsonNodeType nodeType, Object resultValue, Class resultType )
    {
    JsonNode canonical = JSONCoercibleType.TYPE.canonical( value );

    assertEquals( nodeType, canonical.getNodeType() );
    assertEquals( resultValue, JSONCoercibleType.TYPE.coerce( canonical, resultType ) );
    }

  @Test
  public void objectCoercions()
    {
    for( String value : JSONData.objects )
      testContainerCoercion( value, JsonNodeType.OBJECT, String.class );
    }

  @Test
  public void arrayCoercions()
    {
    for( String value : JSONData.arrays )
      testContainerCoercion( value, JsonNodeType.ARRAY, String.class );
    }

  @Test
  public void mapCoercions()
    {
    Map<String, Object> map = new LinkedHashMap<>();

    map.put( "name", "John Doe" );
    map.put( "list", Arrays.asList( "John", "Jane" ) );

    JsonNode canonical = JSONCoercibleType.TYPE.canonical( map );

    assertEquals( JsonNodeType.OBJECT, canonical.getNodeType() );
    assertEquals( map, JSONCoercibleType.TYPE.coerce( canonical, Map.class ) );
    }

  @Test
  public void listCoercions()
    {
    List<Object> list = new LinkedList<>();

    list.add( "John Doe" );
    list.add( Arrays.asList( "John", "Jane" ) );

    JsonNode canonical = JSONCoercibleType.TYPE.canonical( list );

    assertEquals( JsonNodeType.ARRAY, canonical.getNodeType() );
    assertEquals( list, JSONCoercibleType.TYPE.coerce( canonical, List.class ) );
    }

  @Test
  public void pojoCoercions()
    {
    ObjectMapper mapper = new ObjectMapper();

    mapper.registerModule( new JavaTimeModule() );

    JSONCoercibleType type = new JSONCoercibleType( mapper );

    Instant instant = Instant.ofEpochSecond( 1525456424, 337000000 );

    JsonNode canonical = type.canonical( instant );

    String coerce = type.coerce( canonical, String.class );

    assertEquals( "1525456424.337000000", coerce );
    }

  private void testContainerCoercion( String value, JsonNodeType nodeType, Class resultType )
    {
    JsonNode canonical = JSONCoercibleType.TYPE.canonical( value );

    assertEquals( nodeType, canonical.getNodeType() );
    assertEquals( value.replaceAll( "\\s", "" ), JSONCoercibleType.TYPE.coerce( canonical, resultType ) );
    }
  }
