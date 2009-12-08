/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
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
    Object value;

    public Context( Number extrema )
      {
      this.extrema = extrema;
      }

    public Context reset( Number extrema )
      {
      this.extrema = extrema;
      this.value = null;

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

  public void start( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    if( aggregatorCall.getContext() == null )
      aggregatorCall.setContext( new Context( getInitialValue() ) );
    else
      aggregatorCall.getContext().reset( getInitialValue() );
    }

  protected abstract double getInitialValue();

  public void aggregate( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    TupleEntry entry = aggregatorCall.getArguments();
    Context context = aggregatorCall.getContext();

    Comparable arg = entry.get( 0 );

    if( ignoreValues != null && ignoreValues.contains( arg ) )
      return;

    Number rhs = null;

    if( arg instanceof Number )
      rhs = (Number) arg;
    else
      rhs = entry.getDouble( 0 );

    Number lhs = context.extrema;

    if( compare( lhs, rhs ) )
      {
      context.value = arg; // keep and return original value
      context.extrema = rhs;
      }
    }

  protected abstract boolean compare( Number lhs, Number rhs );

  public void complete( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

  protected Tuple getResult( AggregatorCall<Context> aggregatorCall )
    {
    return new Tuple( (Comparable) aggregatorCall.getContext().value );
    }
  }
