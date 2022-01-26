/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

import java.beans.ConstructorProperties;
import java.util.Map;
import java.util.stream.Stream;

import cascading.nested.core.NestedAggregate;
import cascading.nested.core.NestedGetAllAggregateFunction;
import cascading.operation.SerFunction;
import cascading.tuple.Fields;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Class JSONGetAllAggregateFunction provides the ability to retrieve a list of child nodes and
 * convert the specified properties of each JSON object into a new aggregate value rendered by the given implementation
 * of {@link NestedAggregate}.
 * <p>
 * Note the {@code stringRootPointer} may reference a JSON Array, {@code /person/*}, or it may be a pointer-path
 * descent reference, {@code /person/**}{@code /name}. In the later case, use an empty pointer, {@code ""}, to reference
 * the value of the array.
 * <p>
 * See {@link cascading.nested.core.aggregate.SimpleNestedAggregate} for a convenient base implementation.
 *
 * @see <a href=https://tools.ietf.org/html/draft-ietf-appsawg-json-pointer-03">draft-ietf-appsawg-json-pointer-03</a>
 * @see NestedGetAllAggregateFunction for more details.
 */
public class JSONGetAllAggregateFunction extends NestedGetAllAggregateFunction<JsonNode, ArrayNode>
  {
  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param stringRootPointer of type String
   * @param pointerMap        of type Map
   */
  @ConstructorProperties({"stringRootPointer", "pointerMap"})
  public JSONGetAllAggregateFunction( String stringRootPointer, Map<String, NestedAggregate<JsonNode, ?>> pointerMap )
    {
    this( stringRootPointer, false, pointerMap );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param stringRootPointer of type String
   * @param failOnMissingNode of type boolean
   * @param pointerMap        of type Map
   */
  @ConstructorProperties({"stringRootPointer", "failOnMissingNode", "pointerMap"})
  public JSONGetAllAggregateFunction( String stringRootPointer, boolean failOnMissingNode, Map<String, NestedAggregate<JsonNode, ?>> pointerMap )
    {
    super( JSONCoercibleType.TYPE, stringRootPointer, failOnMissingNode, pointerMap );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param coercibleType     of type JSONCoercibleType
   * @param stringRootPointer of type String
   * @param pointerMap        of type Map
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "pointerMap"})
  public JSONGetAllAggregateFunction( JSONCoercibleType coercibleType, String stringRootPointer, Map<String, NestedAggregate<JsonNode, ?>> pointerMap )
    {
    this( coercibleType, stringRootPointer, false, pointerMap );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param coercibleType     of type JSONCoercibleType
   * @param stringRootPointer of type String
   * @param failOnMissingNode of type boolean
   * @param pointerMap        of type Map
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "failOnMissingNode", "pointerMap"})
  public JSONGetAllAggregateFunction( JSONCoercibleType coercibleType, String stringRootPointer, boolean failOnMissingNode, Map<String, NestedAggregate<JsonNode, ?>> pointerMap )
    {
    super( coercibleType, stringRootPointer, failOnMissingNode, pointerMap );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param stringRootPointer of type String
   * @param streamWrapper     of type SerFunction
   * @param pointerMap        of type Map
   */
  @ConstructorProperties({"stringRootPointer", "streamWrapper", "pointerMap"})
  public JSONGetAllAggregateFunction( String stringRootPointer, SerFunction<Stream<JsonNode>, Stream<JsonNode>> streamWrapper, Map<String, NestedAggregate<JsonNode, ?>> pointerMap )
    {
    this( stringRootPointer, streamWrapper, false, pointerMap );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param stringRootPointer of type String
   * @param streamWrapper     of type SerFunction
   * @param failOnMissingNode of type boolean
   * @param pointerMap        of type Map
   */
  @ConstructorProperties({"stringRootPointer", "streamWrapper", "failOnMissingNode", "pointerMap"})
  public JSONGetAllAggregateFunction( String stringRootPointer, SerFunction<Stream<JsonNode>, Stream<JsonNode>> streamWrapper, boolean failOnMissingNode, Map<String, NestedAggregate<JsonNode, ?>> pointerMap )
    {
    super( JSONCoercibleType.TYPE, stringRootPointer, streamWrapper, failOnMissingNode, pointerMap );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param coercibleType     of type JSONCoercibleType
   * @param stringRootPointer of type String
   * @param streamWrapper     of type SerFunction
   * @param pointerMap        of type Map
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "streamWrapper", "pointerMap"})
  public JSONGetAllAggregateFunction( JSONCoercibleType coercibleType, String stringRootPointer, SerFunction<Stream<JsonNode>, Stream<JsonNode>> streamWrapper, Map<String, NestedAggregate<JsonNode, ?>> pointerMap )
    {
    this( coercibleType, stringRootPointer, streamWrapper, false, pointerMap );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param coercibleType     of type JSONCoercibleType
   * @param stringRootPointer of type String
   * @param streamWrapper     of type SerFunction
   * @param failOnMissingNode of type boolean
   * @param pointerMap        of type Map
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "streamWrapper", "failOnMissingNode", "pointerMap"})
  public JSONGetAllAggregateFunction( JSONCoercibleType coercibleType, String stringRootPointer, SerFunction<Stream<JsonNode>, Stream<JsonNode>> streamWrapper, boolean failOnMissingNode, Map<String, NestedAggregate<JsonNode, ?>> pointerMap )
    {
    super( coercibleType, stringRootPointer, streamWrapper, failOnMissingNode, pointerMap );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param stringRootPointer of type String
   * @param fieldDeclaration  of the Fields
   * @param stringPointers    of type String[]
   * @param nestedAggregates  of type NestedAggregate[]
   */
  @ConstructorProperties({"stringRootPointer", "streamWrapper", "fieldDeclaration",
                          "stringPointers", "nestedAggregates"})
  public JSONGetAllAggregateFunction( String stringRootPointer, Fields fieldDeclaration, String[] stringPointers, NestedAggregate<JsonNode, ?>[] nestedAggregates )
    {
    this( stringRootPointer, fieldDeclaration, false, stringPointers, nestedAggregates );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param stringRootPointer of type String
   * @param failOnMissingNode of type boolean
   * @param fieldDeclaration  of the Fields
   * @param failOnMissingNode of type boolean
   * @param stringPointers    of type String[]
   * @param nestedAggregates  of type NestedAggregate[]
   */
  @ConstructorProperties({"stringRootPointer", "fieldDeclaration",
                          "failOnMissingNode", "stringPointers", "nestedAggregates"})
  public JSONGetAllAggregateFunction( String stringRootPointer, Fields fieldDeclaration, boolean failOnMissingNode, String[] stringPointers, NestedAggregate<JsonNode, ?>[] nestedAggregates )
    {
    super( JSONCoercibleType.TYPE, stringRootPointer, fieldDeclaration, failOnMissingNode, stringPointers, nestedAggregates );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param coercibleType     of type JSONCoercibleType
   * @param stringRootPointer of type String
   * @param fieldDeclaration  of the Fields
   * @param stringPointers    of type String[]
   * @param nestedAggregates  of type NestedAggregate[]
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "fieldDeclaration",
                          "stringPointers", "nestedAggregates"})
  public JSONGetAllAggregateFunction( JSONCoercibleType coercibleType, String stringRootPointer, Fields fieldDeclaration, String[] stringPointers, NestedAggregate<JsonNode, ?>[] nestedAggregates )
    {
    this( coercibleType, stringRootPointer, fieldDeclaration, false, stringPointers, nestedAggregates );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param coercibleType     of type JSONCoercibleType
   * @param stringRootPointer of type String
   * @param failOnMissingNode of type boolean
   * @param fieldDeclaration  of the Fields
   * @param failOnMissingNode of type boolean
   * @param stringPointers    of type String[]
   * @param nestedAggregates  of type NestedAggregate[]
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "fieldDeclaration",
                          "failOnMissingNode", "stringPointers", "nestedAggregates"})
  public JSONGetAllAggregateFunction( JSONCoercibleType coercibleType, String stringRootPointer, Fields fieldDeclaration, boolean failOnMissingNode, String[] stringPointers, NestedAggregate<JsonNode, ?>[] nestedAggregates )
    {
    super( coercibleType, stringRootPointer, fieldDeclaration, failOnMissingNode, stringPointers, nestedAggregates );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param stringRootPointer of type String
   * @param streamWrapper     of type SerFunction
   * @param fieldDeclaration  of the Fields
   * @param stringPointers    of type String[]
   * @param nestedAggregates  of type NestedAggregate[]
   */
  @ConstructorProperties({"stringRootPointer", "streamWrapper", "fieldDeclaration",
                          "stringPointers", "nestedAggregates"})
  public JSONGetAllAggregateFunction( String stringRootPointer, SerFunction<Stream<JsonNode>, Stream<JsonNode>> streamWrapper, Fields fieldDeclaration, String[] stringPointers, NestedAggregate<JsonNode, ?>[] nestedAggregates )
    {
    this( stringRootPointer, streamWrapper, fieldDeclaration, false, stringPointers, nestedAggregates );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param stringRootPointer of type String
   * @param streamWrapper     of type SerFunction
   * @param failOnMissingNode of type boolean
   * @param fieldDeclaration  of the Fields
   * @param failOnMissingNode of type boolean
   * @param stringPointers    of type String[]
   * @param nestedAggregates  of type NestedAggregate[]
   */
  @ConstructorProperties({"stringRootPointer", "streamWrapper", "fieldDeclaration",
                          "failOnMissingNode", "stringPointers", "nestedAggregates"})
  public JSONGetAllAggregateFunction( String stringRootPointer, SerFunction<Stream<JsonNode>, Stream<JsonNode>> streamWrapper, Fields fieldDeclaration, boolean failOnMissingNode, String[] stringPointers, NestedAggregate<JsonNode, ?>[] nestedAggregates )
    {
    this( JSONCoercibleType.TYPE, stringRootPointer, streamWrapper, fieldDeclaration, failOnMissingNode, stringPointers, nestedAggregates );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param coercibleType     of type JSONCoercibleType
   * @param stringRootPointer of type String
   * @param streamWrapper     of type SerFunction
   * @param fieldDeclaration  of the Fields
   * @param stringPointers    of type String[]
   * @param nestedAggregates  of type NestedAggregate[]
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "streamWrapper", "fieldDeclaration",
                          "stringPointers", "nestedAggregates"})
  public JSONGetAllAggregateFunction( JSONCoercibleType coercibleType, String stringRootPointer, SerFunction<Stream<JsonNode>, Stream<JsonNode>> streamWrapper, Fields fieldDeclaration, String[] stringPointers, NestedAggregate<JsonNode, ?>[] nestedAggregates )
    {
    this( coercibleType, stringRootPointer, streamWrapper, fieldDeclaration, false, stringPointers, nestedAggregates );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param coercibleType     of type JSONCoercibleType
   * @param stringRootPointer of type String
   * @param streamWrapper     of type SerFunction
   * @param failOnMissingNode of type boolean
   * @param stringPointers    of type String[]
   * @param nestedAggregates  of type NestedAggregate[]
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "streamWrapper",
                          "failOnMissingNode", "stringPointers", "nestedAggregates"})
  public JSONGetAllAggregateFunction( JSONCoercibleType coercibleType, String stringRootPointer, SerFunction<Stream<JsonNode>, Stream<JsonNode>> streamWrapper, boolean failOnMissingNode, String[] stringPointers, NestedAggregate<JsonNode, ?>[] nestedAggregates )
    {
    super( coercibleType, stringRootPointer, streamWrapper, failOnMissingNode, stringPointers, nestedAggregates );
    }

  /**
   * Constructor JSONGetAllAggregateFunction creates a new instance.
   *
   * @param coercibleType     of type JSONCoercibleType
   * @param stringRootPointer of type String
   * @param streamWrapper     of type SerFunction
   * @param fieldDeclaration  of the Fields
   * @param failOnMissingNode of type boolean
   * @param stringPointers    of type String[]
   * @param nestedAggregates  of type NestedAggregate[]
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "streamWrapper", "fieldDeclaration",
                          "failOnMissingNode", "stringPointers", "nestedAggregates"})
  public JSONGetAllAggregateFunction( JSONCoercibleType coercibleType, String stringRootPointer, SerFunction<Stream<JsonNode>, Stream<JsonNode>> streamWrapper, Fields fieldDeclaration, boolean failOnMissingNode, String[] stringPointers, NestedAggregate<JsonNode, ?>[] nestedAggregates )
    {
    super( coercibleType, stringRootPointer, streamWrapper, fieldDeclaration, failOnMissingNode, stringPointers, nestedAggregates );
    }
  }
