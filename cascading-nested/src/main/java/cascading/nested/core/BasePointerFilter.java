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

import java.util.function.Predicate;

import heretical.pointer.path.NestedPointerCompiler;
import heretical.pointer.path.Pointer;

/**
 *
 */
public abstract class BasePointerFilter<Node, Result> implements Predicate<Node>
  {
  final NestedPointerCompiler<Node, Result> nestedPointerCompiler;
  final String stringPointer;

  transient Pointer<Node> pointer;

  public BasePointerFilter( NestedPointerCompiler<Node, Result> nestedPointerCompiler, String pointer )
    {
    this.nestedPointerCompiler = nestedPointerCompiler;
    this.stringPointer = pointer;

    // verify the pointer is valid
    nodePointer();
    }

  protected Pointer<Node> nodePointer()
    {
    if( pointer == null )
      pointer = nestedPointerCompiler.compile( stringPointer );

    return pointer;
    }

  @Override
  public boolean test( Node node )
    {
    return compares( nodePointer().at( node ) );
    }

  protected abstract boolean compares( Node at );
  }
