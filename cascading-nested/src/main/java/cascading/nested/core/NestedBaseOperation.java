/*
 * Copyright (c) 2016-2019 Chris K Wensel. All Rights Reserved.
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

import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.type.CoercibleType;
import heretical.pointer.path.NestedPointerCompiler;

/**
 *
 */
public class NestedBaseOperation<Node, Result, Context> extends BaseOperation<Context>
  {
  protected final NestedCoercibleType<Node, Result> nestedCoercibleType;

  public NestedBaseOperation( NestedCoercibleType<Node, Result> nestedCoercibleType )
    {
    this.nestedCoercibleType = nestedCoercibleType;
    }

  public NestedBaseOperation( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration )
    {
    super( fieldDeclaration.hasTypes() ? fieldDeclaration : fieldDeclaration.applyTypeToAll( nestedCoercibleType ) );
    this.nestedCoercibleType = nestedCoercibleType;
    }

  public NestedBaseOperation( NestedCoercibleType<Node, Result> nestedCoercibleType, int numArgs, Fields fieldDeclaration )
    {
    super( numArgs, fieldDeclaration.hasTypes() ? fieldDeclaration : fieldDeclaration.applyTypeToAll( nestedCoercibleType ) );
    this.nestedCoercibleType = nestedCoercibleType;
    }

  protected NestedPointerCompiler<Node, Result> getNestedPointerCompiler()
    {
    return nestedCoercibleType.getNestedPointerCompiler();
    }

  protected CoercibleType<Node> getCoercibleType()
    {
    return nestedCoercibleType;
    }

  protected Node deepCopy( Node node )
    {
    return nestedCoercibleType.deepCopy( node );
    }

  protected Node getRootNode()
    {
    return nestedCoercibleType.newRoot();
    }

  protected Node getLiteralNode( Object value )
    {
    return nestedCoercibleType.node( value );
    }

  protected Iterable<Node> iterable( Result node )
    {
    return nestedCoercibleType.getNestedPointerCompiler().iterable( node );
    }

  protected int size( Result node )
    {
    return nestedCoercibleType.getNestedPointerCompiler().size( node );
    }
  }
