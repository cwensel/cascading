/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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
   * Constructor JSONGetFunction creates a new JSONGetFunction instance.
   *
   * @param pointerMap of Map
   */
  @ConstructorProperties("pointerMap")
  public JSONGetFunction( Map<Fields, String> pointerMap )
    {
    this( asFields( pointerMap.keySet() ), asArray( pointerMap.values() ) );
    }

  /**
   * Constructor JSONGetFunction creates a new JSONGetFunction instance.
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
   * Constructor JSONGetFunction creates a new JSONGetFunction instance.
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
   * Constructor JSONGetFunction creates a new JSONGetFunction instance.
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
  }
