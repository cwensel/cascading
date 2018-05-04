/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

import cascading.nested.core.NestedGetFunction;
import cascading.tuple.Fields;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Class JSONGetFunction provides for the ability to convert a JSON object into multiple tuple values.
 *
 * @see NestedGetFunction for more details.
 */
public class JSONGetFunction extends NestedGetFunction<JsonNode, ArrayNode>
  {
  /**
   * Creates a new JSONGetFunction instance.
   *
   * @param pointerMap of Map
   */
  @ConstructorProperties("pointerMap")
  public JSONGetFunction( Map<Fields, String> pointerMap )
    {
    this( asFields( pointerMap.keySet() ), asArray( pointerMap.values() ) );
    }

  /**
   * Creates a new JSONGetFunction instance.
   *
   * @param pointerMap        of Map
   * @param failOnMissingNode of boolean
   */
  @ConstructorProperties({"pointerMap", "failOnMissingNode"})
  public JSONGetFunction( Map<Fields, String> pointerMap, boolean failOnMissingNode )
    {
    this( asFields( pointerMap.keySet() ), failOnMissingNode, asArray( pointerMap.values() ) );
    }

  /**
   * Creates a new JSONGetFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param stringPointers   of String...
   */
  @ConstructorProperties({"fieldDeclaration", "stringPointers"})
  public JSONGetFunction( Fields fieldDeclaration, String... stringPointers )
    {
    this( fieldDeclaration, false, stringPointers );
    }

  /**
   * Creates a new JSONGetFunction instance.
   *
   * @param fieldDeclaration  of Fields
   * @param failOnMissingNode of boolean
   * @param stringPointers    of String...
   */
  @ConstructorProperties({"fieldDeclaration", "failOnMissingNode", "stringPointers"})
  public JSONGetFunction( Fields fieldDeclaration, boolean failOnMissingNode, String... stringPointers )
    {
    super( JSONCoercibleType.TYPE, fieldDeclaration, failOnMissingNode, stringPointers );
    }

  /**
   * Creates a new JSONGetFunction instance.
   *
   * @param coercibleType of JSONCoercibleType
   * @param pointerMap    of Map
   */
  @ConstructorProperties({"coercibleType", "pointerMap"})
  public JSONGetFunction( JSONCoercibleType coercibleType, Map<Fields, String> pointerMap )
    {
    this( coercibleType, asFields( pointerMap.keySet() ), asArray( pointerMap.values() ) );
    }

  /**
   * Creates a new JSONGetFunction instance.
   *
   * @param coercibleType     of JSONCoercibleType
   * @param pointerMap        of Map
   * @param failOnMissingNode of boolean
   */
  @ConstructorProperties({"coercibleType", "pointerMap", "failOnMissingNode"})
  public JSONGetFunction( JSONCoercibleType coercibleType, Map<Fields, String> pointerMap, boolean failOnMissingNode )
    {
    this( coercibleType, asFields( pointerMap.keySet() ), failOnMissingNode, asArray( pointerMap.values() ) );
    }

  /**
   * Creates a new JSONGetFunction instance.
   *
   * @param coercibleType    of JSONCoercibleType
   * @param fieldDeclaration of Fields
   * @param stringPointers   of String...
   */
  @ConstructorProperties({"coercibleType", "fieldDeclaration", "stringPointers"})
  public JSONGetFunction( JSONCoercibleType coercibleType, Fields fieldDeclaration, String... stringPointers )
    {
    this( coercibleType, fieldDeclaration, false, stringPointers );
    }

  /**
   * Creates a new JSONGetFunction instance.
   *
   * @param coercibleType     of JSONCoercibleType
   * @param fieldDeclaration  of Fields
   * @param failOnMissingNode of boolean
   * @param stringPointers    of String...
   */
  @ConstructorProperties({"coercibleType", "fieldDeclaration", "failOnMissingNode", "stringPointers"})
  public JSONGetFunction( JSONCoercibleType coercibleType, Fields fieldDeclaration, boolean failOnMissingNode, String... stringPointers )
    {
    super( coercibleType, fieldDeclaration, failOnMissingNode, stringPointers );
    }
  }
