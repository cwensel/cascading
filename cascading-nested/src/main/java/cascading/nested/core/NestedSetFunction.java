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

package cascading.nested.core;

import java.util.Map;

import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Class NestedSetFunction is the base class for {@link Function} implementations that want to simply store
 * values in an existing nested object tree.
 * <p>
 * All argument values referenced by the pointerMap will be set on the root node, a copy of the first argument to this
 * operation.
 * <p>
 * That is, every Fields instance in the pointer map is expected to have a corresponding argument passed to the operation.
 * <p>
 * The pointer path mapped to any given Fields instance in the pointerMap will be used as the location to set on the
 * root node.
 * <p>
 * If a {@code pointerMap} is not provided, the resolved argument fields will be mapped to the root of the node. This is a
 * convenience for quickly pivoting a Tuple into an nested object with the same attributes.
 */
public class NestedSetFunction<Node, Result> extends NestedBaseFunction<Node, Result>
  {
  public NestedSetFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration )
    {
    super( nestedCoercibleType, fieldDeclaration );
    }

  public NestedSetFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration, String rootPointer )
    {
    super( nestedCoercibleType, fieldDeclaration, rootPointer );
    }

  public NestedSetFunction( NestedCoercibleType nestedCoercibleType, Fields fieldDeclaration, Map<Fields, String> pointerMap )
    {
    super( nestedCoercibleType, fieldDeclaration, pointerMap );
    }

  @Override
  protected Node getNode( TupleEntry arguments )
    {
    Node node = (Node) arguments.getObject( 0, getCoercibleType() );

    return deepCopy( node );
    }
  }
