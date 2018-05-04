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

import cascading.nested.core.BuildSpec;
import cascading.nested.core.NestedBaseBuildFunction;
import cascading.tuple.Fields;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Class JSONBuildAsFunction provides for the ability to create new JSON objects from streamed tuple values.
 *
 * @see NestedBaseBuildFunction for more details.
 */
public class JSONBuildAsFunction extends NestedBaseBuildFunction<JsonNode, ArrayNode>
  {
  /**
   * Creates a new JSONBuildAsFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param buildSpecs       of BuildSpec...
   */
  @ConstructorProperties({"fieldDeclaration", "buildSpecs"})
  public JSONBuildAsFunction( Fields fieldDeclaration, BuildSpec... buildSpecs )
    {
    super( JSONCoercibleType.TYPE, fieldDeclaration, buildSpecs );
    }

  /**
   * Creates a new JSONBuildAsFunction instance.
   *
   * @param coercibleType    of JSONCoercibleType
   * @param fieldDeclaration of Fields
   * @param buildSpecs       of BuildSpec...
   */
  @ConstructorProperties({"coercibleType", "fieldDeclaration", "buildSpecs"})
  public JSONBuildAsFunction( JSONCoercibleType coercibleType, Fields fieldDeclaration, BuildSpec... buildSpecs )
    {
    super( coercibleType, fieldDeclaration, buildSpecs );
    }

  /**
   * Method isInto returns the into of this JSONBuildAsFunction object.
   *
   * @return the into (type boolean) of this JSONBuildAsFunction object.
   */
  @Override
  protected boolean isInto()
    {
    return false;
    }
  }
