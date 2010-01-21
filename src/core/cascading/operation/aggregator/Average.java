/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/** Class Average is an {@link Aggregator} that returns the average of all numeric values in the current group. */
public class Average extends BaseOperation<Average.Context> implements Aggregator<Average.Context>
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "average";

  /** Class Context is used to hold intermediate values. */
  protected static class Context
    {
    double sum = 0.0D;
    long count = 0L;

    public Context reset()
      {
      sum = 0.0D;
      count = 0L;

      return this;
      }
    }

  /** Constructs a new instance that returns the average of the values encoutered in the field name "average". */
  public Average()
    {
    super( 1, new Fields( FIELD_NAME ) );
    }

  /**
   * Constructs a new instance that returns the average of the values encoutered in the given fieldDeclaration field name.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public Average( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
    }

  public void start( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    if( aggregatorCall.getContext() != null )
      aggregatorCall.getContext().reset();
    else
      aggregatorCall.setContext( new Context() );
    }

  public void aggregate( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    Context context = aggregatorCall.getContext();
    TupleEntry arguments = aggregatorCall.getArguments();

    context.sum += arguments.getDouble( 0 );
    context.count += 1d;
    }

  public void complete( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
    {
    aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

  private Tuple getResult( AggregatorCall<Context> aggregatorCall )
    {
    Context context = aggregatorCall.getContext();

    return new Tuple( (Double) context.sum / context.count );
    }
  }
