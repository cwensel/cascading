/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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

import cascading.nested.core.CopySpec;
import cascading.nested.core.NestedBaseCopyFunction;
import cascading.tuple.Fields;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Class JSONCopyAsFunction provides for the ability to create new JSON objects from an existing
 * JSON object.
 *
 * @see NestedBaseCopyFunction for more details.
 */
public class JSONCopyAsFunction extends NestedBaseCopyFunction<JsonNode, ArrayNode>
  {
  /**
   * Constructor JSONCopyAsFunction creates a new JSONCopyAsFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param copySpecs        of CopySpec...
   */
  @ConstructorProperties({"fieldDeclaration", "copySpecs"})
  public JSONCopyAsFunction( Fields fieldDeclaration, CopySpec... copySpecs )
    {
    super( JSONCoercibleType.TYPE, fieldDeclaration, copySpecs );
    }

  @Override
  protected boolean isInto()
    {
    return false;
    }
  }
