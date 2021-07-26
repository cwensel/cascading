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

import java.util.Collection;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.LazyIterable;
import cascading.util.Pair;
import heretical.pointer.path.NestedPointer;
import heretical.pointer.path.NestedPointerCompiler;

/**
 * Class NestedGetAllAggregateFunction is the base class for {@link Function} implementations that when given
 * the root of a collection of container nodes, need to aggregate the values of a common child element.
 * <p>
 * For example, given an array of objects that represent a person, the function can calculate the average
 * age of the people listed in the array if every object has the same property name for {@code age}.
 * <p>
 * The {@code stringRootPointer} value must point to a container node with one or more child elements or objects.
 * <p>
 * The {@code pointerMap} maps a child node or property found in the child object to a {@link NestedAggregateFunction}
 * implementation that implements the required aggregation algorithm.
 * <p>
 * If {@code failOnMissingNode} is {@code true} and the root pointer is empty or the field pointer returns a
 * {@code null} value, the operation will fail.
 */
public class NestedGetAllAggregateFunction<Node, Result> extends NestedGetFunction<Node, Result>
  {
  protected final NestedPointer<Node, Result> rootPointer;
  protected final NestedAggregateFunction<Node, ?>[] nestedAggregateFunctions;

  /**
   * Constructor NestedGetAllAggregateFunction creates a new NestedGetAllAggregateFunction instance.
   *
   * @param nestedCoercibleType of type NestedCoercibleType
   * @param stringRootPointer   of type String
   * @param failOnMissingNode   of type boolean
   * @param pointerMap          of type Map
   */
  public NestedGetAllAggregateFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, String stringRootPointer, boolean failOnMissingNode, Map<String, NestedAggregateFunction<Node, ?>> pointerMap )
    {
    super( nestedCoercibleType, declared( pointerMap.values() ), failOnMissingNode, asArray( pointerMap.keySet() ) );

    NestedPointerCompiler<Node, Result> compiler = getNestedPointerCompiler();

    this.rootPointer = compiler.nested( stringRootPointer );
    this.nestedAggregateFunctions = pointerMap.values().toArray( new NestedAggregateFunction[ 0 ] );
    }

  protected static <Node> Fields declared( Collection<NestedAggregateFunction<Node, ?>> nestedAggregateFunctions )
    {
    return nestedAggregateFunctions.stream().map( NestedAggregateFunction::getFieldDeclaration ).reduce( Fields.NONE, Fields::append );
    }

  @Override
  protected void verify( String[] stringPointers )
    {
    // do nothing as each NestedAggregateFunction may return more than one field
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Tuple> operationCall )
    {
    Pair<NestedAggregateFunction<Node, ?>, Object>[] pairs = new Pair[ nestedAggregateFunctions.length ];

    for( int i = 0; i < nestedAggregateFunctions.length; i++ )
      pairs[ i ] = new Pair<>( nestedAggregateFunctions[ i ], nestedAggregateFunctions[ i ].createContext() );

    operationCall.setContext( new Tuple( Tuple.size( getFieldDeclaration().size() ), pairs ) );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Tuple> functionCall )
    {
    Node argument = (Node) functionCall.getArguments().getObject( 0, getCoercibleType() );

    Result result = rootPointer.allAt( argument );

    if( failOnMissingNode && getNestedPointerCompiler().size( result ) == 0 )
      throw new OperationException( "nodes missing from json node tree: " + rootPointer );

    Tuple resultTuple = (Tuple) functionCall.getContext().getObject( 0 );
    Pair<NestedAggregateFunction<Node, Object>, Object>[] pairs = (Pair<NestedAggregateFunction<Node, Object>, Object>[]) functionCall.getContext().getObject( 1 );

    for( Pair<NestedAggregateFunction<Node, Object>, Object> pair : pairs )
      pair.setRhs( pair.getLhs().resetContext( pair.getRhs() ) );

    for( Node node : getNestedPointerCompiler().iterable( result ) )
      aggregateNode( pairs, node );

    resultTuple.setAll( new LazyIterable<Pair<NestedAggregateFunction<Node, Object>, Object>, Tuple>( pairs )
      {
      @Override
      protected Tuple convert( Pair<NestedAggregateFunction<Node, Object>, Object> next )
        {
        return next.getLhs().complete( next.getRhs() );
        }
      } );

    functionCall.getOutputCollector().add( resultTuple );
    }

  protected void aggregateNode( Pair<NestedAggregateFunction<Node, Object>, Object>[] pairs, Node node )
    {
    extractResult( ( i, value ) -> setInto( pairs, i, value ), node );
    }

  protected void setInto( Pair<NestedAggregateFunction<Node, Object>, Object>[] pairs, int i, Node result )
    {
    NestedAggregateFunction<Node, Object> nestedAggregateFunction = pairs[ i ].getLhs();
    Object context = pairs[ i ].getRhs();

    nestedAggregateFunction.aggregate( context, getCoercibleType(), result );
    }
  }
