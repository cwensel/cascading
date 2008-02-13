/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

import java.util.Map;

import cascading.operation.Aggregator;
import cascading.operation.Operation;
import cascading.pipe.Operator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/**
 * Concrete implementation of {@link Operator} that acts as an {@link Aggregator} by
 * returning the minimum value over a set of {@link TupleEntry}
 */
public class Min extends Operation implements Aggregator
  {
  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;
  /** Field FIELD_NAME */
  private static final String FIELD_NAME = "min";

  /** Constructor */
  public Min()
    {
    super( 1, new Fields( FIELD_NAME ) );
    }

  /** @see Aggregator#start(java.util.Map,cascading.tuple.TupleEntry) */
  @SuppressWarnings("unchecked")
  public void start( Map context, TupleEntry groupEntry )
    {
    context.put( FIELD_NAME, Double.MAX_VALUE );
    }

  /** @see Aggregator#aggregate(Map, TupleEntry) */
  @SuppressWarnings("unchecked")
  public void aggregate( Map context, TupleEntry entry )
    {
    final Double val = entry.getTuple().getDouble( 0 );

    if( (Double) context.get( FIELD_NAME ) > val )
      context.put( FIELD_NAME, val );
    }

  /** @see Aggregator#complete(java.util.Map,cascading.tuple.TupleCollector) */
  @SuppressWarnings("unchecked")
  public void complete( Map context, TupleCollector outputCollector )
    {
    outputCollector.add( new Tuple( (Double) context.get( FIELD_NAME ) ) );
    }
  }
