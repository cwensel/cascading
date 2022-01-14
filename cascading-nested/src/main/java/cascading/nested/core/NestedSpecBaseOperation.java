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

import java.util.function.Function;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 *
 */
public abstract class NestedSpecBaseOperation<Node, Result, Context> extends NestedBaseOperation<Node, Result, Context>
  {
  transient Function<OperationCall<Context>, Node> resultNodeFunction = null;

  public NestedSpecBaseOperation( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration )
    {
    super( nestedCoercibleType, fieldDeclaration );
    }

  public NestedSpecBaseOperation( NestedCoercibleType<Node, Result> nestedCoercibleType, int numArgs, Fields fieldDeclaration )
    {
    super( nestedCoercibleType, numArgs, fieldDeclaration );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    if( isInto() )
      resultNodeFunction = this::getArgument;
    else
      resultNodeFunction = o -> getRootNode();
    }

  protected abstract boolean isInto();

  protected Node getResultNode( OperationCall<Context> operationCall )
    {
    return resultNodeFunction.apply( operationCall );
    }

  protected Node getArgument( OperationCall<?> operationCall )
    {
    TupleEntry arguments = ( (FunctionCall<Object>) operationCall ).getArguments();

    Node object = (Node) arguments.getObject( arguments.size() - 1, getCoercibleType() );

    return deepCopy( object );
    }
  }
