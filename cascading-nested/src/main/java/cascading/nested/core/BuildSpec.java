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

import java.lang.reflect.Type;

import heretical.pointer.operation.CopySpec;

/**
 * Class BuildSpec is used to declare how a key value maps into a new or existing nested object type.
 * <p>
 * To map child elements from one nested object to another, see {@link CopySpec} and related operations.
 * <p>
 * When using a BuildSpec, you are declaring that a field value is put into a specific location in a nested object
 * type.
 * <p>
 * When a BuildSpec is created, the target root location of all the values must be declared, or all values will
 * be placed immediately below the root object.
 * <p>
 * For example, you want to put a field named {@code firstName} it into a JSON tree at {@code /person/firstName}
 * there are two ways to use a BuildSpec.
 * <p>
 * Either
 * <p>
 * {@code
 * new BuildSpec().putInto( "firstName", "/person/firstName" );
 * }
 * <p>
 * Or
 * <p>
 * {@code
 * new BuildSpec( "/person" ).putInto( "firstName", "/firstName" );
 * }
 * <p>
 * Note that a field being copied or put into the new object can also be a nested object. In the case of JSON
 * if the object to be copied is a JSON String, the value can be converted to a JSON object on the copy.
 * <p>
 * {@code
 * new BuildSpec().putInto( "person", JSONCoercibleType.TYPE, "/person" );
 * }
 * <p>
 * This example assumes the {@code person} field is a valid JSON String or already a JSON {@code JsonNode} instance.
 *
 * @see CopySpec
 */
public class BuildSpec extends heretical.pointer.operation.BuildSpec<BuildSpec>
  {
  public BuildSpec()
    {
    }

  public BuildSpec( Type defaultType )
    {
    super( defaultType );
    }

  public BuildSpec( String intoPointer )
    {
    super( intoPointer );
    }

  public BuildSpec( String intoPointer, Type defaultType )
    {
    super( intoPointer, defaultType );
    }
  }
