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

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import cascading.CascadingException;
import cascading.nested.core.NestedCoercibleType;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;
import cascading.tuple.type.CoercionFrom;
import cascading.tuple.type.SerializableType;
import cascading.tuple.type.ToCanonical;
import cascading.util.Util;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import heretical.pointer.path.NestedPointerCompiler;
import heretical.pointer.path.json.JSONNestedPointerCompiler;

/**
 * Class JSONCoercibleType is a {@link NestedCoercibleType} that provides support for JSON object types.
 * <p>
 * Supported values will be maintained as a {@link JsonNode} canonical type within the {@link cascading.tuple.Tuple}.
 * <p>
 * Note that {@link #canonical(Object)} will always attempt to parse a String value to a new JsonNode.
 * If the parse fails, it will return a {@link com.fasterxml.jackson.databind.node.TextNode} instance wrapping the
 * String value.
 * <p>
 * Note the default instance (@link {@link #TYPE}), sets the {@link DeserializationFeature#FAIL_ON_READING_DUP_TREE_KEY}
 * Jackson property.
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

  /**
   * Instantiates a new Json coercible type with the given {@link ObjectMapper} instance.
   * <p>
   * Use this constructor in order to leverage {@link Module} instances.
   * <p>
   * Note the default instance (@link {@link #TYPE}), sets the {@link DeserializationFeature#FAIL_ON_READING_DUP_TREE_KEY}
   * Jackson property.
   *
   * @param mapper the mapper
   */
  public JSONCoercibleType( ObjectMapper mapper )
    {
    this.mapper = mapper;
    }

  @Override
  public Class<JsonNode> getCanonicalType()
    {
    return JsonNode.class;
    }

  @Override
  public <T> ToCanonical<T, JsonNode> from( Type from )
    {
    if( from.getClass() == JSONCoercibleType.class )
      return ( v ) -> (JsonNode) v;

    if( from instanceof Class && JsonNode.class.isAssignableFrom( (Class<?>) from ) )
      return ( v ) -> (JsonNode) v;

    if( from == String.class )
      return ( v ) -> v == null ? null : nodeOrParse( (String) v );

    if( from == Short.class || from == Short.TYPE )
      return ( v ) -> v == null ? null : JsonNodeFactory.instance.numberNode( (Short) v );

    if( from == Integer.class || from == Integer.TYPE )
      return ( v ) -> v == null ? null : JsonNodeFactory.instance.numberNode( (Integer) v );

    if( from == Long.class || from == Long.TYPE )
      return ( v ) -> v == null ? null : JsonNodeFactory.instance.numberNode( (Long) v );

    if( from == Float.class || from == Float.TYPE )
      return ( v ) -> v == null ? null : JsonNodeFactory.instance.numberNode( (Float) v );

    if( from == Double.class || from == Double.TYPE )
      return ( v ) -> v == null ? null : JsonNodeFactory.instance.numberNode( (Double) v );

    if( from == Boolean.class || from == Boolean.TYPE )
      return ( v ) -> v == null ? null : JsonNodeFactory.instance.booleanNode( (Boolean) v );

    if( from instanceof Class && ( Collection.class.isAssignableFrom( (Class<?>) from ) || Map.class.isAssignableFrom( (Class<?>) from ) ) )
      return ( v ) -> v == null ? null : mapper.valueToTree( v );

    return ( v ) -> v == null ? null : JsonNodeFactory.instance.pojoNode( v );
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

    if( from == Short.class || from == Short.TYPE )
      return JsonNodeFactory.instance.numberNode( (Short) value );

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

    return JsonNodeFactory.instance.pojoNode( value );
    }

  protected <T> T ifNull( JsonNode node, Function<JsonNode, T> function )
    {
    if( node == null || node.getNodeType() == JsonNodeType.NULL || node.getNodeType() == JsonNodeType.MISSING )
      return null;

    return function.apply( node );
    }

  @Override
  public <T> CoercionFrom<JsonNode, T> to( Type to )
    {
    if( to == null || to.getClass() == JSONCoercibleType.class )
      return t -> (T) t;

    Class<?> actualTo;

    if( to instanceof CoercibleType )
      actualTo = ( (CoercibleType<?>) to ).getCanonicalType();
    else
      actualTo = (Class<?>) to;

    if( JsonNode.class.isAssignableFrom( actualTo ) )
      return t -> (T) t;

    if( actualTo == String.class )
      return node -> ifNull( node, n -> n.getNodeType() == JsonNodeType.STRING ? (T) n.textValue() : (T) textOrWrite( n ) );

    if( actualTo == Short.class || actualTo == Short.TYPE )
      return node -> ifNull( node, n -> n.getNodeType() == JsonNodeType.NUMBER ? (T) Short.valueOf( n.shortValue() ) : Coercions.coerce( textOrWrite( n ), to ) );

    if( actualTo == Integer.class || actualTo == Integer.TYPE )
      return node -> ifNull( node, n -> n.getNodeType() == JsonNodeType.NUMBER ? (T) Integer.valueOf( n.intValue() ) : Coercions.coerce( textOrWrite( n ), to ) );

    if( actualTo == Long.class || actualTo == Long.TYPE )
      return node -> ifNull( node, n -> n.getNodeType() == JsonNodeType.NUMBER ? (T) Long.valueOf( n.longValue() ) : Coercions.coerce( textOrWrite( n ), to ) );

    if( actualTo == Float.class || actualTo == Float.TYPE )
      return node -> ifNull( node, n -> n.getNodeType() == JsonNodeType.NUMBER ? (T) Float.valueOf( n.floatValue() ) : Coercions.coerce( textOrWrite( n ), to ) );

    if( actualTo == Double.class || actualTo == Double.TYPE )
      return node -> ifNull( node, n -> n.getNodeType() == JsonNodeType.NUMBER ? (T) Double.valueOf( n.doubleValue() ) : Coercions.coerce( textOrWrite( n ), to ) );

    if( actualTo == Boolean.class || actualTo == Boolean.TYPE )
      return node -> ifNull( node, n -> n.getNodeType() == JsonNodeType.BOOLEAN ? (T) Boolean.valueOf( n.booleanValue() ) : Coercions.coerce( textOrWrite( n ), to ) );

    if( Map.class.isAssignableFrom( actualTo ) )
      return node -> ifNull( node, n -> (T) convert( n, actualTo ) );

    if( List.class.isAssignableFrom( actualTo ) )
      return node -> ifNull( node, n -> (T) convert( n, actualTo ) );

    if( to instanceof CoercibleType )
      return node -> ifNull( node, n -> (T) ( (CoercibleType<?>) to ).canonical( textOrWrite( n ) ) );

    throw new CascadingException( "unknown type coercion requested, from: " + Util.getTypeName( JsonNode.class ) + " to: " + Util.getTypeName( to ) );
    }

  @Override
  public <Coerce> Coerce coerce( Object value, Type to )
    {
    if( to == null || to.getClass() == JSONCoercibleType.class )
      return (Coerce) value;

    if( value == null )
      return null;

    Class<?> from = value.getClass();

    if( !JsonNode.class.isAssignableFrom( from ) )
      throw new IllegalStateException( "was not normalized, got: " + from.getName() );

    JsonNode node = (JsonNode) value;

    if( node.isMissingNode() )
      return null;

    JsonNodeType nodeType = node.getNodeType();

    if( nodeType == JsonNodeType.NULL )
      return null;

    Class<?> actualTo;

    if( to instanceof CoercibleType )
      actualTo = ( (CoercibleType<?>) to ).getCanonicalType();
    else
      actualTo = (Class<?>) to;

    // support sub-classes of JsonNode
    if( JsonNode.class.isAssignableFrom( actualTo ) )
      return (Coerce) node;

    if( actualTo == String.class )
      return nodeType == JsonNodeType.STRING ? (Coerce) node.textValue() : (Coerce) textOrWrite( node );

    if( actualTo == Short.class || actualTo == Short.TYPE )
      return nodeType == JsonNodeType.NUMBER ? (Coerce) Short.valueOf( node.shortValue() ) : (Coerce) Coercions.coerce( textOrWrite( node ), to );

    if( actualTo == Integer.class || actualTo == Integer.TYPE )
      return nodeType == JsonNodeType.NUMBER ? (Coerce) Integer.valueOf( node.intValue() ) : (Coerce) Coercions.coerce( textOrWrite( node ), to );

    if( actualTo == Long.class || actualTo == Long.TYPE )
      return nodeType == JsonNodeType.NUMBER ? (Coerce) Long.valueOf( node.longValue() ) : (Coerce) Coercions.coerce( textOrWrite( node ), to );

    if( actualTo == Float.class || actualTo == Float.TYPE )
      return nodeType == JsonNodeType.NUMBER ? (Coerce) Float.valueOf( node.floatValue() ) : (Coerce) Coercions.coerce( textOrWrite( node ), to );

    if( actualTo == Double.class || actualTo == Double.TYPE )
      return nodeType == JsonNodeType.NUMBER ? (Coerce) Double.valueOf( node.doubleValue() ) : (Coerce) Coercions.coerce( textOrWrite( node ), to );

    if( actualTo == Boolean.class || actualTo == Boolean.TYPE )
      return nodeType == JsonNodeType.BOOLEAN ? (Coerce) Boolean.valueOf( node.booleanValue() ) : (Coerce) Coercions.coerce( textOrWrite( node ), to );

    if( Map.class.isAssignableFrom( actualTo ) )
      return (Coerce) convert( value, actualTo );

    if( List.class.isAssignableFrom( actualTo ) )
      return (Coerce) convert( value, actualTo );

    if( to instanceof CoercibleType )
      return (Coerce) ( (CoercibleType<?>) to ).canonical( textOrWrite( node ) );

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
      return cascading.nested.json.hadoop3.JSONHadoopSerialization.class;

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
