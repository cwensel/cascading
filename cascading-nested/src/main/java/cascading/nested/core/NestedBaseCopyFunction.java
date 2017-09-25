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

import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;
import heretical.pointer.operation.Copier;

/**
 * Class NestedBaseCopyFunction is the base class for {@link Function} implementations that rely on the
 * {@link CopySpec} class when declaring transformations on nested object types.
 * <p>
 * Specifically, {@code *CopyAsFunction} and {@code *CopyIntoFunction} classes create or update (respectively)
 * nested object types from a Function argument where the field value is a nest object type.
 * <p>
 * In the case of a {@code *CopyIntoFunction} the last argument in the {@code arguments} {@link TupleEntry} will be
 * the object the CopySpec copies values into.
 * <p>
 * In the case of a {@code *CopyAsFunction} a new root object will be created for the CopySpec to copy values into.
 * <p>
 * Note the arguments TupleEntry will be passed to any {@link Transform} instances that are resettable
 * {@link Transform#isResettable()} allowing for parameterized transformations on child values as they are copied
 * to the new location.
 * <p>
 * In the case of JSON objects, a single JSON object is selected as an argument so that values contained in that object
 * can be copied into the new object.
 * <p>
 * For selecting the values from multiple existing field values in order to create a new object or update an existing one
 * see {@link NestedBaseBuildFunction} sub-classes.
 *
 * @see CopySpec
 */
public abstract class NestedBaseCopyFunction<Node, Result> extends NestedSpecBaseOperation<Node, Result, NestedBaseCopyFunction.Context> implements Function<NestedBaseCopyFunction.Context>
  {
  protected static class Context
    {
    public Tuple result;
    public Fields fields;

    public Context( Tuple result, Fields fields )
      {
      this.result = result;
      this.fields = fields;
      }
    }

  protected Copier<Node, Result> copier;

  public NestedBaseCopyFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration, CopySpec... copySpecs )
    {
    super( nestedCoercibleType, fieldDeclaration );
    this.copier = new Copier<>( getNestedPointerCompiler(), copySpecs );

    if( fieldDeclaration.isDefined() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "can only return a single field" );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    operationCall.setContext( new Context( Tuple.size( 1 ), operationCall.getArgumentFields().subtract( Fields.FIRST ) ) );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Context> functionCall )
    {
    TupleEntry arguments = functionCall.getArguments();
    Node fromNode = (Node) arguments.getObject( 0, getCoercibleType() );

    if( arguments.size() > ( isInto() ? 2 : 1 ) )
      resetTransforms( arguments, functionCall.getContext().fields );

    Node resultNode = getResultNode( functionCall );

    copier.copy( fromNode, resultNode );

    Context context = functionCall.getContext();

    context.result.set( 0, resultNode );

    functionCall.getOutputCollector().add( context.result );
    }

  protected void resetTransforms( TupleEntry arguments, Fields fields )
    {
    Map<Comparable, Object> values = Tuples.asComparableMap( fields, arguments );

    copier.resetTransforms( values );
    }
  }
