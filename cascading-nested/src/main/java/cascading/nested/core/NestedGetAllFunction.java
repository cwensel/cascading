/*
 * Copyright (c) 2016-2021 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import heretical.pointer.path.NestedPointer;
import heretical.pointer.path.NestedPointerCompiler;

/**
 * Class NestedGetAllFunction is the base class for {@link Function} implementations that want to simply retrieve
 * a collection of values in nested object, and then transform each element node into a tuple with declared fields.
 * <p>
 * The {@code stringRootPointer} value must point to a container node with one or more elements. Each element
 * will have values extracted and a new tuple will be emitted for each element.
 * <p>
 * For every field named in the fieldDeclaration {@link Fields} argument, there must be a corresponding
 * {@code stringPointer} value.
 * <p>
 * If {@code failOnMissingNode} is {@code true} and the root pointer is empty or the field pointer returns a
 * {@code null} value, the operation will fail.
 * <p>
 * If the fieldDeclaration Fields instance declares a type information, the {@code nestedCoercibleType} will be used
 * to coerce any referenced child value to the expected field type.
 */
public class NestedGetAllFunction<Node, Result> extends NestedGetFunction<Node, Result>
  {
  protected NestedPointer<Node, Result> rootPointer;

  /**
   * Constructor NestedGetAllFunction creates a new NestedGetFunction instance.
   *
   * @param nestedCoercibleType of NestedCoercibleType
   * @param stringRootPointer   of type String
   * @param fieldDeclaration    of type Fields
   * @param failOnMissingNode   of type boolean
   * @param stringPointers      of type String...
   */
  public NestedGetAllFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, String stringRootPointer, Fields fieldDeclaration, boolean failOnMissingNode, String... stringPointers )
    {
    super( nestedCoercibleType, fieldDeclaration, failOnMissingNode, stringPointers );

    NestedPointerCompiler<Node, Result> compiler = getNestedPointerCompiler();

    this.rootPointer = compiler.nested( stringRootPointer );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Tuple> functionCall )
    {
    Tuple resultTuple = functionCall.getContext();
    Node argument = (Node) functionCall.getArguments().getObject( 0, getCoercibleType() );

    Result result = rootPointer.allAt( argument );

    if( failOnMissingNode && getNestedPointerCompiler().size( result ) == 0 )
      throw new OperationException( "nodes missing from json node tree: " + rootPointer );

    Iterable<Node> iterable = getNestedPointerCompiler().iterable( result );

    for( Node node : iterable )
      {
      extractResult( resultTuple, node );

      functionCall.getOutputCollector().add( resultTuple );
      }
    }
  }
