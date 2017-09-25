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

package cascading.nested.core;

/**
 * Class CopySpec is used to declare a mapping of parts of one nested object into a new or existing nested object type.
 * <p>
 * To map fields into a nested object, see {@link BuildSpec} and related operations.
 * <p>
 * When using a CopySpec, you are declaring which sub-trees from an existing nested object are copied into
 * another nested object.
 * <p>
 * When a CopySpec is created, the target root location of all the values must be declared, or all values will
 * be placed immediately below the root object.
 * <p>
 * For example, you want to copy a person named {@code John Doe} into a JSON object.
 * <p>
 * {@code
 * new CopySpec().from( "/people/*", new JSONStringPointerFilter( "/person/name", "John Doe" ) );
 * }
 * <p>
 * This example assumes the {@code people} object is an array of {@code person} objects.
 */
public class CopySpec extends heretical.pointer.operation.CopySpec<CopySpec>
  {
  public CopySpec()
    {
    }

  public CopySpec( String intoPointer )
    {
    super( intoPointer );
    }
  }
