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

import java.beans.ConstructorProperties;
import java.util.Map;

import cascading.nested.core.NestedGetAllFunction;
import cascading.tuple.Fields;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Class JSONGetAllFunction provides the ability to retrieve a list of child nodes and
 * convert each JSON object into tuple of values.
 *
 * Note the {@code stringRootPointer} may reference a JSON Array, {@code /person/*}, or it may be a pointer-path
 * descent reference, {@code /person/**}{@code /name}. In the later case, use an empty pointer, {@code ""}, to reference
 * the value of the array. Rely on the {@code fieldDeclaration} to coerce this value appropriately.
 *
 * @see <a href=https://tools.ietf.org/html/draft-ietf-appsawg-json-pointer-03">draft-ietf-appsawg-json-pointer-03</a>
 * @see NestedGetAllFunction for more details.
 */
public class JSONGetAllFunction extends NestedGetAllFunction<JsonNode, ArrayNode>
  {
  /**
   * Creates a new JSONGetAllFunction instance.
   *
   * @param stringRootPointer of String
   * @param pointerMap of Map
   */
  @ConstructorProperties({"stringRootPointer", "pointerMap"})
  public JSONGetAllFunction( String stringRootPointer, Map<Fields, String> pointerMap )
    {
    this( stringRootPointer, asFields( pointerMap.keySet() ), asArray( pointerMap.values() ) );
    }

  /**
   * Creates a new JSONGetAllFunction instance.
   *
   * @param stringRootPointer of String
   * @param pointerMap        of Map
   * @param failOnMissingNode of boolean
   */
  @ConstructorProperties({"stringRootPointer", "pointerMap", "failOnMissingNode"})
  public JSONGetAllFunction( String stringRootPointer, Map<Fields, String> pointerMap, boolean failOnMissingNode )
    {
    this( stringRootPointer, asFields( pointerMap.keySet() ), failOnMissingNode, asArray( pointerMap.values() ) );
    }

  /**
   * Creates a new JSONGetAllFunction instance.
   *
   * @param stringRootPointer of String
   * @param fieldDeclaration of Fields
   * @param stringPointers   of String...
   */
  @ConstructorProperties({"stringRootPointer", "fieldDeclaration", "stringPointers"})
  public JSONGetAllFunction( String stringRootPointer, Fields fieldDeclaration, String... stringPointers )
    {
    this( stringRootPointer, fieldDeclaration, false, stringPointers );
    }

  /**
   * Creates a new JSONGetAllFunction instance.
   *
   * @param stringRootPointer of String
   * @param fieldDeclaration  of Fields
   * @param failOnMissingNode of boolean
   * @param stringPointers    of String...
   */
  @ConstructorProperties({"stringRootPointer", "fieldDeclaration", "failOnMissingNode", "stringPointers"})
  public JSONGetAllFunction( String stringRootPointer, Fields fieldDeclaration, boolean failOnMissingNode, String... stringPointers )
    {
    super( JSONCoercibleType.TYPE, stringRootPointer, fieldDeclaration, failOnMissingNode, stringPointers );
    }

  /**
   * Creates a new JSONGetAllFunction instance.
   *
   * @param coercibleType of JSONCoercibleType
   * @param stringRootPointer of String
   * @param pointerMap    of Map
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "pointerMap"})
  public JSONGetAllFunction( JSONCoercibleType coercibleType, String stringRootPointer, Map<Fields, String> pointerMap )
    {
    this( coercibleType, stringRootPointer, asFields( pointerMap.keySet() ), asArray( pointerMap.values() ) );
    }

  /**
   * Creates a new JSONGetAllFunction instance.
   *
   * @param coercibleType     of JSONCoercibleType
   * @param stringRootPointer of String
   * @param pointerMap        of Map
   * @param failOnMissingNode of boolean
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "pointerMap", "failOnMissingNode"})
  public JSONGetAllFunction( JSONCoercibleType coercibleType, String stringRootPointer, Map<Fields, String> pointerMap, boolean failOnMissingNode )
    {
    this( coercibleType, stringRootPointer, asFields( pointerMap.keySet() ), failOnMissingNode, asArray( pointerMap.values() ) );
    }

  /**
   * Creates a new JSONGetAllFunction instance.
   *
   * @param coercibleType    of JSONCoercibleType
   * @param stringRootPointer of String
   * @param fieldDeclaration of Fields
   * @param stringPointers   of String...
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "fieldDeclaration", "stringPointers"})
  public JSONGetAllFunction( JSONCoercibleType coercibleType, String stringRootPointer, Fields fieldDeclaration, String... stringPointers )
    {
    this( coercibleType, stringRootPointer, fieldDeclaration, false, stringPointers );
    }

  /**
   * Creates a new JSONGetAllFunction instance.
   *
   * @param coercibleType     of JSONCoercibleType
   * @param stringRootPointer of String
   * @param fieldDeclaration  of Fields
   * @param failOnMissingNode of boolean
   * @param stringPointers    of String...
   */
  @ConstructorProperties({"coercibleType", "stringRootPointer", "fieldDeclaration", "failOnMissingNode",
                          "stringPointers"})
  public JSONGetAllFunction( JSONCoercibleType coercibleType, String stringRootPointer, Fields fieldDeclaration, boolean failOnMissingNode, String... stringPointers )
    {
    super( coercibleType, stringRootPointer, fieldDeclaration, failOnMissingNode, stringPointers );
    }
  }
