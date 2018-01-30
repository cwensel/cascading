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

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import cascading.CascadingException;
import cascading.nested.core.NestedCoercibleType;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;
import cascading.tuple.type.SerializableType;
import cascading.util.Util;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import heretical.pointer.path.NestedPointerCompiler;
import heretical.pointer.path.json.JSONNestedPointerCompiler;

/**
 * Class JSONCoercibleType is a {@link NestedCoercibleType} that provides support
 * for JSON object types.
 * <p>
 * Supported values will be maintained as a {@link JsonNode} canonical type within the {@link cascading.tuple.Tuple}.
 * <p>
 * Note that {@link #canonical(Object)} will always attempt to parse a String value to a new JsonNode.
 * If the parse fails, it will return a {@link com.fasterxml.jackson.databind.node.TextNode} instance wrapping the
 * String value.
 * <p>
 * See the {@link #node(Object)}.
 */
public class JSONCoercibleType implements NestedCoercibleType<JsonNode, ArrayNode>, SerializableType
  {
  public static final JSONCoercibleType TYPE = new JSONCoercibleType();

  private ObjectMapper mapper = new ObjectMapper();

  private JSONCoercibleType()
    {
    // prevents json object from being created with duplicate names at the same level
    mapper.setConfig( mapper.getDeserializationConfig()
      .with( DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY ) );
    }

  @Override
  public Class<JsonNode> getCanonicalType()
    {
    return JsonNode.class;
    }

  @Override
  public JsonNode canonical( Object value )
    {
    if( value == null )
      return null;

    Class from = value.getClass();

    if( JsonNode.class.isAssignableFrom( from ) )
      return (JsonNode) value;

    if( from == String.class )
      return nodeOrParse( (String) value );

    if( from == Integer.class || from == Integer.TYPE )
      return JsonNodeFactory.instance.numberNode( (Integer) value );

    if( from == Long.class || from == Long.TYPE )
      return JsonNodeFactory.instance.numberNode( (Long) value );

    if( from == Float.class || from == Float.TYPE )
      return JsonNodeFactory.instance.numberNode( (Float) value );

    if( from == Double.class || from == Double.TYPE )
      return JsonNodeFactory.instance.numberNode( (Double) value );

    if( from == Boolean.class || from == Boolean.TYPE )
      return JsonNodeFactory.instance.booleanNode( (Boolean) value );

    if( Collection.class.isAssignableFrom( from ) || Map.class.isAssignableFrom( from ) )
      return mapper.valueToTree( value );

    throw new CascadingException( "unknown type coercion requested from: " + Util.getTypeName( from ) );
    }

  @Override
  public <Coerce> Coerce coerce( Object value, Type to )
    {
    if( to == null || to.getClass() == JSONCoercibleType.class )
      return (Coerce) value;

    if( value == null )
      return null;

    Class from = value.getClass();

    if( !JsonNode.class.isAssignableFrom( from ) )
      throw new IllegalStateException( "was not normalized, got: " + from.getName() );

    JsonNode node = (JsonNode) value;

    if( node.isMissingNode() )
      return null;

    JsonNodeType nodeType = node.getNodeType();

    if( nodeType == JsonNodeType.NULL )
      return null;

    if( to == String.class )
      return nodeType == JsonNodeType.STRING ? (Coerce) node.textValue() : (Coerce) textOrWrite( node );

    if( to == Integer.class || to == Integer.TYPE )
      return nodeType == JsonNodeType.NUMBER ? (Coerce) Integer.valueOf( node.intValue() ) : (Coerce) Coercions.coerce( textOrWrite( node ), to );

    if( to == Long.class || to == Long.TYPE )
      return nodeType == JsonNodeType.NUMBER ? (Coerce) Long.valueOf( node.longValue() ) : (Coerce) Coercions.coerce( textOrWrite( node ), to );

    if( to == Float.class || to == Float.TYPE )
      return nodeType == JsonNodeType.NUMBER ? (Coerce) Float.valueOf( node.floatValue() ) : (Coerce) Coercions.coerce( textOrWrite( node ), to );

    if( to == Double.class || to == Double.TYPE )
      return nodeType == JsonNodeType.NUMBER ? (Coerce) Double.valueOf( node.doubleValue() ) : (Coerce) Coercions.coerce( textOrWrite( node ), to );

    if( to == Boolean.class || to == Boolean.TYPE )
      return nodeType == JsonNodeType.BOOLEAN ? (Coerce) Boolean.valueOf( node.booleanValue() ) : (Coerce) Coercions.coerce( textOrWrite( node ), to );

    if( Map.class.isAssignableFrom( (Class<?>) to ) )
      return (Coerce) convert( value, (Class) to );

    if( List.class.isAssignableFrom( (Class<?>) to ) )
      return (Coerce) convert( value, (Class) to );

    throw new CascadingException( "unknown type coercion requested, from: " + Util.getTypeName( from ) + " to: " + Util.getTypeName( to ) );
    }

  private Object convert( Object value, Class to )
    {
    return mapper.convertValue( value, to );
    }

  private String textOrWrite( JsonNode value )
    {
    if( value != null && value.isTextual() )
      return value.textValue();

    try
      {
      return write( value );
      }
    catch( JsonProcessingException exception )
      {
      throw new CascadingException( "unable to write value as json", exception );
      }
    }

  private String write( JsonNode value ) throws JsonProcessingException
    {
    return mapper.writeValueAsString( value );
    }

  private JsonNode nodeOrParse( String value )
    {
    try
      {
      return parse( value ); // presume this is a JSON string
      }
    catch( JsonParseException exception )
      {
      return JsonNodeFactory.instance.textNode( value );
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to read json", exception );
      }
    }

  private JsonNode parse( String value ) throws IOException
    {
    return mapper.readTree( value );
    }

  @Override
  public NestedPointerCompiler<JsonNode, ArrayNode> getNestedPointerCompiler()
    {
    return JSONNestedPointerCompiler.COMPILER;
    }

  @Override
  public JsonNode deepCopy( JsonNode jsonNode )
    {
    if( jsonNode == null )
      return null;

    return jsonNode.deepCopy();
    }

  @Override
  public JsonNode newRoot()
    {
    return JsonNodeFactory.instance.objectNode();
    }

  @Override
  public Class getSerializer( Class base )
    {
    // required to defer classloading
    if( base == org.apache.hadoop.io.serializer.Serialization.class )
      return cascading.nested.json.hadoop2.JSONHadoopSerialization.class;

    return null;
    }

  @Override
  public String toString()
    {
    return getClass().getName();
    }

  @Override
  public int hashCode()
    {
    return getCanonicalType().hashCode();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;

    if( !( object instanceof CoercibleType ) )
      return false;

    return getCanonicalType().equals( ( (CoercibleType) object ).getCanonicalType() );
    }
  }
