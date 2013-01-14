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
import java.lang.reflect.Type;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;
import cascading.util.Pair;

/** Class Sum is an {@link Aggregator} that returns the sum of all numeric values in the current group. */
public class Sum extends BaseOperation<Pair<Double[], Tuple>> implements Aggregator<Pair<Double[], Tuple>>
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "sum";

  /** Field type */
  private Type type = double.class;
  private CoercibleType canonical;


  /** Constructor Sum creates a new Sum instance that accepts one argument and returns a single field named "sum". */
  public Sum()
    {
    super( 1, new Fields( FIELD_NAME ) );
    this.canonical = Coercions.coercibleTypeFor( this.type );
    }

  /**
   * Constructs a new instance that returns the fields declared in fieldDeclaration and accepts
   * only 1 argument.
   * <p/>
   * If the given {@code fieldDeclaration} has a type, it will be used to coerce the result value.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public Sum( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );

    if( fieldDeclaration.hasTypes() )
      this.type = fieldDeclaration.getType( 0 );

    this.canonical = Coercions.coercibleTypeFor( this.type );
    }

  /**
   * Constructs a new instance that returns the fields declared in fieldDeclaration and accepts
   * only 1 argument. The return result is coerced into the given Class type.
   *
   * @param fieldDeclaration of type Fields
   * @param type             of type Class
   */
  @ConstructorProperties({"fieldDeclaration", "type"})
  public Sum( Fields fieldDeclaration, Class type )
    {
    this( fieldDeclaration );
    this.type = type;
    this.canonical = Coercions.coercibleTypeFor( this.type );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Pair<Double[], Tuple>> operationCall )
    {
    operationCall.setContext( new Pair<Double[], Tuple>( new Double[]{null}, Tuple.size( 1 ) ) );
    }

  @Override
  public void start( FlowProcess flowProcess, AggregatorCall<Pair<Double[], Tuple>> aggregatorCall )
    {
    aggregatorCall.getContext().getLhs()[ 0 ] = null;
    aggregatorCall.getContext().getRhs().set( 0, null );
    }

  @Override
  public void aggregate( FlowProcess flowProcess, AggregatorCall<Pair<Double[], Tuple>> aggregatorCall )
    {
    TupleEntry arguments = aggregatorCall.getArguments();

    if( arguments.getObject( 0 ) == null )
      return;

    Double[] sum = aggregatorCall.getContext().getLhs();

    double value = sum[ 0 ] == null ? 0 : sum[ 0 ];
    sum[ 0 ] = value + arguments.getDouble( 0 );
    }

  @Override
  public void complete( FlowProcess flowProcess, AggregatorCall<Pair<Double[], Tuple>> aggregatorCall )
    {
    aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

  protected Tuple getResult( AggregatorCall<Pair<Double[], Tuple>> aggregatorCall )
    {
    aggregatorCall.getContext().getRhs().set( 0, canonical.canonical( aggregatorCall.getContext().getLhs()[ 0 ] ) );

    return aggregatorCall.getContext().getRhs();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof Sum ) )
      return false;
    if( !super.equals( object ) )
      return false;

    Sum sum = (Sum) object;

    if( type != null ? !type.equals( sum.type ) : sum.type != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( type != null ? type.hashCode() : 0 );
    return result;
    }
  }
