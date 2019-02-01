/*
 * Copyright (c) 2018-2019 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.local.tap.neo4j;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import heretical.pointer.path.Pointer;

import static heretical.pointer.path.json.JSONNestedPointerCompiler.COMPILER;

/**
 *
 */
public class JSONGraphSpec implements GraphSpec
  {
  public static class Ref
    {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String fromPointer;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Object defaultValue;
    @JsonIgnore
    Function<JsonNode, Object> function;

    public Ref( Function<JsonNode, Object> function )
      {
      this.function = function;
      }

    public Ref( Object defaultValue )
      {
      this( null, defaultValue );
      }

    @JsonCreator
    public Ref( @JsonProperty("fromPointer") String fromPointer, @JsonProperty("defaultValue") Object defaultValue )
      {
      this.fromPointer = fromPointer;
      this.defaultValue = defaultValue;

      if( fromPointer != null )
        {
        Pointer<JsonNode> pointer = COMPILER.compile( fromPointer );
        function = n -> pointer.at( n ) == null ? defaultValue : JSONUtil.at( pointer, n );
        }
      else
        {
        function = n -> defaultValue;
        }
      }

    public String getFromPointer()
      {
      return fromPointer;
      }

    public Object getDefaultValue()
      {
      return defaultValue;
      }

    public Function<JsonNode, Object> getFunction()
      {
      return function;
      }

    @Override
    public boolean equals( Object o )
      {
      if( this == o )
        return true;
      if( !( o instanceof Ref ) )
        return false;
      Ref ref = (Ref) o;
      return Objects.equals( fromPointer, ref.fromPointer ) &&
        Objects.equals( defaultValue, ref.defaultValue ) &&
        Objects.equals( function, ref.function );
      }

    @Override
    public int hashCode()
      {
      return Objects.hash( fromPointer, defaultValue, function );
      }
    }

  public static class EdgeSpec
    {
    String edgeType;
    String targetLabel;
    Map<String, Ref> targetProperties = new LinkedHashMap<>(); // used for match or create

    public EdgeSpec()
      {
      }

    public EdgeSpec( String edgeType )
      {
      this.edgeType = edgeType;
      }

    @JsonCreator
    public EdgeSpec( @JsonProperty("edgeType") String edgeType, @JsonProperty("targetLabel") String targetLabel )
      {
      this( edgeType );
      this.targetLabel = targetLabel;
      }

    public boolean hasEdgeType()
      {
      return getEdgeType() != null;
      }

    public String getEdgeType()
      {
      return edgeType;
      }

    public boolean hasTargetLabel()
      {
      return getTargetLabel() != null;
      }

    public String getTargetLabel()
      {
      return targetLabel;
      }

    public boolean hasTargetProperties()
      {
      return !targetProperties.isEmpty();
      }

    @JsonGetter("targetProperties")
    public Map<String, Ref> getRefTargetProperties()
      {
      return targetProperties;
      }

    public Map<String, Function<JsonNode, Object>> getTargetProperties()
      {
      return targetProperties.entrySet().stream().collect( Collectors.toMap( Map.Entry::getKey, e -> e.getValue().function ) );
      }

    public EdgeSpec addTargetLabel( String targetLabel )
      {
      this.targetLabel = targetLabel;

      return this;
      }

    public EdgeSpec addTargetProperty( String property, Object value )
      {
      return addTargetProperty( property, new Ref( value ) );
      }

    public EdgeSpec addTargetProperty( String property, String fromPointer, Object defaultValue )
      {
      return addTargetProperty( property, new Ref( fromPointer, defaultValue ) );
      }

    public EdgeSpec addTargetProperty( String property, Function<JsonNode, Object> function )
      {
      return addTargetProperty( property, new Ref( function ) );
      }

    @JsonSetter("targetProperties")
    public EdgeSpec addTargetProperty( String property, Ref ref )
      {
      if( targetProperties.put( property, ref ) != null )
        {
        throw new IllegalArgumentException( "match property: " + property + ", already exists" );
        }

      return this;
      }

    @Override
    public boolean equals( Object o )
      {
      if( this == o )
        return true;
      if( !( o instanceof EdgeSpec ) )
        return false;
      EdgeSpec edgeSpec = (EdgeSpec) o;
      return Objects.equals( edgeType, edgeSpec.edgeType ) &&
        Objects.equals( targetLabel, edgeSpec.targetLabel ) &&
        Objects.equals( targetProperties, edgeSpec.targetProperties );
      }

    @Override
    public int hashCode()
      {
      return Objects.hash( edgeType, targetLabel, targetProperties );
      }
    }

  String nodeLabel;
  Map<String, Ref> properties = new LinkedHashMap<>(); // used for match or create
  Set<EdgeSpec> edges = new LinkedHashSet<>();
  String valuesPointer = null; // return root by default
  Function<JsonNode, JsonNode> valueRef = n -> n;

  @JsonCreator
  public JSONGraphSpec( @JsonProperty("nodeLabel") String nodeLabel )
    {
    this.nodeLabel = nodeLabel;
    }

  public String getNodeLabel()
    {
    return nodeLabel;
    }

  public boolean hasNodeLabel()
    {
    return getNodeLabel() != null;
    }

  @JsonGetter("properties")
  public Map<String, Ref> getRefProperties()
    {
    return properties;
    }

  @JsonIgnore
  public Map<String, Function<JsonNode, Object>> getProperties()
    {
    return properties.entrySet().stream().collect( Collectors.toMap( Map.Entry::getKey, e -> e.getValue().function ) );
    }

  public boolean hasProperties()
    {
    return !properties.isEmpty();
    }

  public JSONGraphSpec addProperty( String property, Object value )
    {
    return addProperty( property, new Ref( value ) );
    }

  public JSONGraphSpec addProperty( String property, String fromPointer, Object defaultValue )
    {
    return addProperty( property, new Ref( fromPointer, defaultValue ) );
    }

  public JSONGraphSpec addProperty( String property, Function<JsonNode, Object> function )
    {
    return addProperty( property, new Ref( function ) );
    }

  @JsonSetter("properties")
  public JSONGraphSpec addProperty( String property, Ref ref )
    {
    if( properties.put( property, ref ) != null )
      throw new IllegalArgumentException( "match property: " + property + " already exists" );

    return this;
    }

  @JsonIgnore
  public Function<JsonNode, JsonNode> getValuesPointer()
    {
    return valueRef;
    }

  @JsonGetter("valuesPointer")
  public String getRefValuesPointer()
    {
    return valuesPointer;
    }

  @JsonSetter("valuesPointer")
  public JSONGraphSpec setValuesPointer( String valuesPointer )
    {
    this.valuesPointer = valuesPointer;
    this.valueRef = n -> n.at( JsonPointer.compile( valuesPointer ) );

    return this;
    }

  @JsonGetter("edges")
  public Set<EdgeSpec> getEdges()
    {
    return edges;
    }

  public boolean hasEdges()
    {
    return !getEdges().isEmpty();
    }

  public EdgeSpec addEdge()
    {
    return addEdge( (String) null );
    }

  public EdgeSpec addEdge( String edgeType )
    {
    return addEdge( new EdgeSpec( edgeType ) );
    }

  public EdgeSpec addEdge( EdgeSpec edgeSpec )
    {
    edges.add( edgeSpec );

    return edgeSpec;
    }

  @JsonSetter("edges")
  public void setEdges( Set<EdgeSpec> edges )
    {
    this.edges.addAll( edges );
    }

  @Override
  public boolean equals( Object o )
    {
    if( this == o )
      return true;
    if( !( o instanceof JSONGraphSpec ) )
      return false;
    JSONGraphSpec graphSpec = (JSONGraphSpec) o;
    return Objects.equals( nodeLabel, graphSpec.nodeLabel ) &&
      Objects.equals( properties, graphSpec.properties ) &&
      Objects.equals( edges, graphSpec.edges ) &&
      Objects.equals( valuesPointer, graphSpec.valuesPointer );
    }

  @Override
  public int hashCode()
    {
    return Objects.hash( nodeLabel, properties, edges, valuesPointer );
    }
  }
