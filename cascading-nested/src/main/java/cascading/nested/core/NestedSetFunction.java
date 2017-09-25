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

import java.util.LinkedHashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import heretical.pointer.path.NestedPointerCompiler;
import heretical.pointer.path.Pointer;

/**
 * Class NestedSetFunction is the base class for {@link Function} implementations that want to simply store
 * values in a nested object tree.
 * <p>
 * All argument values referenced by the pointerMap will be set on the root node, a copy of the first argument to this
 * operation.
 * <p>
 * That is, every Fields instance in the pointer map is expected to have a corresponding argument passed to the operation.
 * <p>
 * The pointer path mapped to any given Fields instance in the pointerMap will be used as the location to set on the
 * root node.
 */
public class NestedSetFunction<Node, Result> extends NestedBaseOperation<Node, Result, Tuple> implements Function<Tuple>
  {
  protected Map<Fields, Pointer<Node>> pointers;

  public NestedSetFunction( NestedCoercibleType nestedCoercibleType, Fields fieldDeclaration, Map<Fields, String> pointerMap )
    {
    super( nestedCoercibleType, fieldDeclaration.hasTypes() ? fieldDeclaration : fieldDeclaration.applyTypeToAll( nestedCoercibleType ) );

    this.pointers = new LinkedHashMap<>();

    NestedPointerCompiler compiler = getNestedPointerCompiler();

    for( Map.Entry<Fields, String> entry : pointerMap.entrySet() )
      this.pointers.put( entry.getKey(), compiler.compile( entry.getValue() ) );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Tuple> operationCall )
    {
    operationCall.setContext( Tuple.size( 1 ) );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Tuple> functionCall )
    {
    Node node = (Node) functionCall.getArguments().getObject( 0, getCoercibleType() );

    node = deepCopy( node );

    for( Map.Entry<Fields, Pointer<Node>> entry : pointers.entrySet() )
      {
      Object value = functionCall.getArguments().getObject( entry.getKey() );
      Node result = getLiteralNode( value );

      entry.getValue().set( node, result );
      }

    functionCall.getContext().set( 0, node );

    functionCall.getOutputCollector().add( functionCall.getContext() );
    }
  }
