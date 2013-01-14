/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.aggregator;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Pair;

/**
 * Class Count is an {@link Aggregator} that calculates the number of items in the current group.
 * </p>
 * Note the resulting value for count is always a long. So any comparisons should be against a long value.
 */
public class Count extends BaseOperation<Pair<Long[], Tuple>> implements Aggregator<Pair<Long[], Tuple>>
  {
  /** Field COUNT */
  public static final String FIELD_NAME = "count";

  /** Constructor Count creates a new Count instance using the default field declaration of name 'count'. */
  public Count()
    {
    super( new Fields( FIELD_NAME ) );
    }

  /**
   * Constructor Count creates a new Count instance and returns a field with the given fieldDeclaration name.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public Count( Fields fieldDeclaration )
    {
    super( fieldDeclaration ); // allow ANY number of arguments
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Pair<Long[], Tuple>> operationCall )
    {
    operationCall.setContext( new Pair<Long[], Tuple>( new Long[]{0L}, Tuple.size( 1 ) ) );
    }

  @Override
  public void start( FlowProcess flowProcess, AggregatorCall<Pair<Long[], Tuple>> aggregatorCall )
    {
    aggregatorCall.getContext().getLhs()[ 0 ] = 0L;
    }

  @Override
  public void aggregate( FlowProcess flowProcess, AggregatorCall<Pair<Long[], Tuple>> aggregatorCall )
    {
    aggregatorCall.getContext().getLhs()[ 0 ] += 1L;
    }

  @Override
  public void complete( FlowProcess flowProcess, AggregatorCall<Pair<Long[], Tuple>> aggregatorCall )
    {
    aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

  protected Tuple getResult( AggregatorCall<Pair<Long[], Tuple>> aggregatorCall )
    {
    aggregatorCall.getContext().getRhs().set( 0, aggregatorCall.getContext().getLhs()[ 0 ] );

    return aggregatorCall.getContext().getRhs();
    }
  }
