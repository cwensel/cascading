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
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import heretical.pointer.operation.Builder;

/**
 * Class NestedBaseBuildAggregator is the base class for {@link Aggregator} implementations that rely on the
 * {@link BuildSpec} class when declaring transformations on nested object types.
 * <p>
 * Specifically, {@code *BuildAsAggregator} and {@code *BuildIntoAggregator} classes create or update (respectively)
 * nested object types from Aggregator argument {@link Fields} where the field values can be primitive types, objects
 * (with a corresponding {@link cascading.tuple.type.CoercibleType}), or themselves nest object types.
 * <p>
 * In the case of a {@code *BuildIntoAggregator} the last argument in the {@code arguments} {@link TupleEntry} will be
 * the object the BuildSpec copies values into.
 * <p>
 * In the case of a {@code *BuildAsAggregator} a new root object will be created for the BuildSpec to copy values into.
 * <p>
 * In the case of JSON objects, multiple JSON objects selected as arguments can be combined into a new JSON object
 * by mapping the field names into locations on the new object.
 *
 * @see BuildSpec
 */
public abstract class NestedBaseBuildAggregator<Node, Result> extends NestedSpecBaseOperation<Node, Result, NestedBaseBuildAggregator<Node, Result>.Context> implements Aggregator<NestedBaseBuildAggregator<Node, Result>.Context>
  {
  public class Context
    {
    Node rootNode = null;

    public void reset()
      {
      rootNode = getResultNode( null );
      }
    }

  protected Builder<Node, Result> builder;

  public NestedBaseBuildAggregator( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration, BuildSpec... buildSpecs )
    {
    super( nestedCoercibleType, fieldDeclaration );
    this.builder = new Builder<>( nestedCoercibleType.getNestedPointerCompiler(), buildSpecs );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    operationCall.setContext( new Context() );
    }

  @Override
  public void start( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    aggregatorCall.getContext().reset();

    Node rootNode = aggregatorCall.getContext().rootNode;

    builder.buildLiterals( rootNode );
    }

  @Override
  public void aggregate( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    TupleEntry arguments = aggregatorCall.getArguments();

    Node rootNode = aggregatorCall.getContext().rootNode;

    builder.build( arguments::getObject, rootNode );
    }

  @Override
  public void complete( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    aggregatorCall.getOutputCollector().add( new Tuple( aggregatorCall.getContext().rootNode ) );
    }
  }
