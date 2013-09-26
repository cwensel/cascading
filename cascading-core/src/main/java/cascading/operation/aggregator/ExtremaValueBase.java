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
 * Class ExtremaValueBase is the base class for MaxValue and MinValue where the values are expected to by
 * {@link Comparable} types and the {@link Comparable#compareTo(Object)} result is use for max/min
 * comparison.
 */
public abstract class ExtremaValueBase extends BaseOperation<ExtremaValueBase.Context> implements Aggregator<ExtremaValueBase.Context>
  {
  /** Field ignoreValues */
  protected final Collection ignoreValues;

  protected static class Context
    {
    Tuple value = Tuple.size( 1 );

    public Context()
      {
      }

    public Context reset()
      {
      this.value.set( 0, null );

      return this;
      }
    }

  @ConstructorProperties({"fieldDeclaration"})
  public ExtremaValueBase( Fields fieldDeclaration )
    {
    super( fieldDeclaration );
    ignoreValues = null;
    }

  @ConstructorProperties({"numArgs", "fieldDeclaration"})
  public ExtremaValueBase( int numArgs, Fields fieldDeclaration )
    {
    super( numArgs, fieldDeclaration );
    ignoreValues = null;

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
    }

  @ConstructorProperties({"fieldDeclaration", "ignoreValues"})
  protected ExtremaValueBase( Fields fieldDeclaration, Object... ignoreValues )
    {
    super( fieldDeclaration );
    this.ignoreValues = new HashSet();
    Collections.addAll( this.ignoreValues, ignoreValues );
    }

  public Collection getIgnoreValues()
    {
    return Collections.unmodifiableCollection( ignoreValues );
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
    }

  @Override
  public void aggregate( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    TupleEntry entry = aggregatorCall.getArguments();
    Context context = aggregatorCall.getContext();

    Object arg = entry.getObject( 0 ); // returns canonical type

    if( ignoreValues != null && ignoreValues.contains( arg ) )
      return;

    Comparable lhs = (Comparable) context.value.getObject( 0 );
    Comparable rhs = (Comparable) arg;

    if( lhs == null || compare( lhs, rhs ) )
      context.value.set( 0, rhs );
    }

  /**
   * Allows subclasses to provide own comparison method.
   *
   * @param lhs Comparable type
   * @param rhs Comparable type
   * @return true if the rhs should be retained as the result value
   */
  protected abstract boolean compare( Comparable lhs, Comparable rhs );

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
    if( !( object instanceof ExtremaValueBase ) )
      return false;
    if( !super.equals( object ) )
      return false;

    ExtremaValueBase that = (ExtremaValueBase) object;

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
