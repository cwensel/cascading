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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class ExtremaBase is the base class for Max and Min. The unique thing about Max and Min are that they return the original,
 * un-coerced, argument value, though a coerced version of the argument is used for the comparison.
 */
public abstract class ExtremaBase extends BaseOperation<ExtremaBase.Context> implements Aggregator<ExtremaBase.Context>
  {
  /** Field ignoreValues */
  protected final Collection ignoreValues;

  protected static class Context
    {
    Number extrema;
    Tuple value = Tuple.size( 1 );

    public Context( Number extrema )
      {
      this.extrema = extrema;
      }

    public Context reset( Number extrema )
      {
      this.extrema = extrema;
      this.value.set( 0, null );

      return this;
      }
    }

  @ConstructorProperties({"fieldDeclaration"})
  public ExtremaBase( Fields fieldDeclaration )
    {
    super( fieldDeclaration );
    ignoreValues = null;
    }

  @ConstructorProperties({"numArgs", "fieldDeclaration"})
  public ExtremaBase( int numArgs, Fields fieldDeclaration )
    {
    super( numArgs, fieldDeclaration );
    ignoreValues = null;

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
    }

  @ConstructorProperties({"fieldDeclaration", "ignoreValues"})
  protected ExtremaBase( Fields fieldDeclaration, Object... ignoreValues )
    {
    super( fieldDeclaration );
    this.ignoreValues = new HashSet();
    Collections.addAll( this.ignoreValues, ignoreValues );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    operationCall.setContext( new Context( getInitialValue() ) );
    }

  @Override
  public void start( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    aggregatorCall.getContext().reset( getInitialValue() );
    }

  protected abstract double getInitialValue();

  @Override
  public void aggregate( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    TupleEntry entry = aggregatorCall.getArguments();
    Context context = aggregatorCall.getContext();

    Object arg = entry.getObject( 0 );

    if( ignoreValues != null && ignoreValues.contains( arg ) )
      return;

    Number rhs;

    if( arg instanceof Number )
      rhs = (Number) arg;
    else
      rhs = entry.getDouble( 0 );

    Number lhs = context.extrema;

    if( compare( lhs, rhs ) )
      {
      context.value.set( 0, arg ); // keep and return original value
      context.extrema = rhs;
      }
    }

  protected abstract boolean compare( Number lhs, Number rhs );

  @Override
  public void complete( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

  protected Tuple getResult( AggregatorCall<Context> aggregatorCall )
    {
    return aggregatorCall.getContext().value;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof ExtremaBase ) )
      return false;
    if( !super.equals( object ) )
      return false;

    ExtremaBase that = (ExtremaBase) object;

    if( ignoreValues != null ? !ignoreValues.equals( that.ignoreValues ) : that.ignoreValues != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( ignoreValues != null ? ignoreValues.hashCode() : 0 );
    return result;
    }
  }
