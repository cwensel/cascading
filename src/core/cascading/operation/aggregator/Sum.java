/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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
import cascading.tuple.Tuples;

/** Class Sum is an {@link Aggregator} that returns the sum of all numeric values in the current group. */
public class Sum extends BaseOperation<Double[]> implements Aggregator<Double[]>
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "sum";

  /** Field type */
  private Class type = double.class;

  /** Constructor Sum creates a new Sum instance that accepts one argument and returns a single field named "sum". */
  public Sum()
    {
    super( 1, new Fields( FIELD_NAME ) );
    }

  /**
   * Constructs a new instance that returns the fields declared in fieldDeclaration and accepts
   * only 1 argument.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public Sum( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
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
    }

  public void start( FlowProcess flowProcess, AggregatorCall<Double[]> aggregatorCall )
    {
    if( aggregatorCall.getContext() == null )
      aggregatorCall.setContext( new Double[]{0.0D} );
    else
      aggregatorCall.getContext()[ 0 ] = 0.0D;
    }

  public void aggregate( FlowProcess flowProcess, AggregatorCall<Double[]> aggregatorCall )
    {
    aggregatorCall.getContext()[ 0 ] += aggregatorCall.getArguments().getDouble( 0 );
    }

  public void complete( FlowProcess flowProcess, AggregatorCall<Double[]> aggregatorCall )
    {
    aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

  protected Tuple getResult( AggregatorCall<Double[]> aggregatorCall )
    {
    Tuple tuple = new Tuple( (Comparable) aggregatorCall.getContext()[ 0 ] );
    return new Tuple( (Comparable) Tuples.coerce( tuple, 0, type ) );
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
