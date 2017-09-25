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

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import heretical.pointer.operation.Builder;

/**
 * Class NestedBaseBuildFunction is the base class for {@link Function} implementations that rely on the
 * {@link BuildSpec} class when declaring transformations on nested object types.
 * <p>
 * Specifically, {@code *BuildAsFunction} and {@code *BuildIntoFunction} classes create or update (respectively)
 * nested object types from Function argument {@link Fields} where the field values can be primitive types, objects
 * (with a corresponding {@link cascading.tuple.type.CoercibleType}), or themselves nest object types.
 * <p>
 * In the case of a {@code *BuildIntoFunction} the last argument in the {@code arguments} {@link TupleEntry} will be
 * the object the BuildSpec copies values into.
 * <p>
 * In the case of a {@code *BuildAsFunction} a new root object will be created for the BuildSpec to copy values into.
 * <p>
 * In the case of JSON objects, multiple JSON objects selected as arguments can be combined into a new JSON object
 * by mapping the field names into locations on the new object.
 * <p>
 * For selecting the values from an existing nested object in order to create a new object or update an existing one
 * see {@link NestedBaseCopyFunction} sub-classes.
 *
 * @see BuildSpec
 */
public abstract class NestedBaseBuildFunction<Node, Result> extends NestedSpecBaseOperation<Node, Result, NestedBaseBuildFunction.Context> implements Function<NestedBaseBuildFunction.Context>
  {
  protected static class Context
    {
    public Tuple result;

    public Context( Tuple result )
      {
      this.result = result;
      }
    }

  protected Builder<Node, Result> builder;

  public NestedBaseBuildFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration, BuildSpec... buildSpecs )
    {
    super( nestedCoercibleType, fieldDeclaration );
    this.builder = new Builder( nestedCoercibleType.getNestedPointerCompiler(), buildSpecs );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    operationCall.setContext( new Context( Tuple.size( 1 ) ) );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Context> functionCall )
    {
    Node rootNode = getResultNode( functionCall );

    TupleEntry arguments = functionCall.getArguments();

    builder.build( arguments::getObject, rootNode );

    Context context = functionCall.getContext();

    context.result.set( 0, rootNode );

    functionCall.getOutputCollector().add( context.result );
    }
  }
