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

import java.util.Collections;
import java.util.Map;

import cascading.nested.core.NestedSetFunction;
import cascading.tuple.Fields;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Class JSONSetFunction provides for the ability to simply set multiple tuple values onto an existing JSON object.
 *
 * @see NestedSetFunction for more details.
 */
public class JSONSetFunction extends NestedSetFunction<JsonNode, ArrayNode>
  {
  /**
   * Constructor JSONSetFunction creates a new JSONSetFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param fromFields       of Fields
   * @param stringPointer    of String
   */
  public JSONSetFunction( Fields fieldDeclaration, Fields fromFields, String stringPointer )
    {
    this( fieldDeclaration, Collections.singletonMap( fromFields, stringPointer ) );
    }

  /**
   * Constructor JSONSetFunction creates a new JSONSetFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param pointerMap       of Map
   */
  public JSONSetFunction( Fields fieldDeclaration, Map<Fields, String> pointerMap )
    {
    super( JSONCoercibleType.TYPE, fieldDeclaration, pointerMap );
    }
  }
