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

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class Count is an {@link Aggregator} that calculates the number of items in the current group.
 * </p>
 * Note the resulting value for count is always a long. So any comparisons should be against a long value.
 */
public class Count extends BaseOperation<Long[]> implements Aggregator<Long[]>
  {
  /** Field COUNT */
  public static final String FIELD_NAME = "count";

  /** Constructor Count creates a new Count instance using the defalt field declaration of name 'count'. */
  public Count()
    {
    super( new Fields( FIELD_NAME ) );
    }

  /**
   * Constructor Count creates a new Count instance and returns a field with the given fieldDeclaration name.
   *
   * @param fieldDeclaration of type Fields
   */
  public Count( Fields fieldDeclaration )
    {
    super( fieldDeclaration ); // allow ANY number of arguments
    }

  public void start( FlowProcess flowProcess, AggregatorCall<Long[]> aggregatorCall )
    {
    if( aggregatorCall.getContext() == null )
      aggregatorCall.setContext( new Long[]{0L} );
    else
      aggregatorCall.getContext()[ 0 ] = 0L;
    }

  public void aggregate( FlowProcess flowProcess, AggregatorCall<Long[]> aggregatorCall )
    {
    aggregatorCall.getContext()[ 0 ] += 1L;
    }

  public void complete( FlowProcess flowProcess, AggregatorCall<Long[]> aggregatorCall )
    {
    aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

  protected Tuple getResult( AggregatorCall<Long[]> aggregatorCall )
    {
    return new Tuple( (Comparable) aggregatorCall.getContext()[ 0 ] );
    }
  }
