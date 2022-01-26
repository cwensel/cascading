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

package cascading.nested.json.transform;

import heretical.pointer.operation.Transform;

/**
 * Class JSONSetTextTransform is an implementation of {@link Transform} for updating the text in a
 * JSON {@link com.fasterxml.jackson.databind.node.TextNode}
 */
public class JSONSetTextTransform extends heretical.pointer.operation.json.transform.JSONSetTextTransform
  {
  /**
   * Constructor JSONSetTextTransform creates a new JSONSetTextTransform instance.
   */
  public JSONSetTextTransform()
    {
    }

  /**
   * Constructor JSONSetTextTransform creates a new JSONSetTextTransform instance.
   *
   *  @param replace of String
   */
  public JSONSetTextTransform( String replace )
    {
    super( replace );
    }

  /**
   * Constructor JSONSetTextTransform creates a new JSONSetTextTransform instance.
   *
   *  @param name of String
   *  @param defaultReplace of String
   */
  public JSONSetTextTransform( String name, String defaultReplace )
    {
    super( name, defaultReplace );
    }
  }
