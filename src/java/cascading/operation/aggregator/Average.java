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
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryListIterator;

/**
 * A concrete type of {@link Aggregator} that considers the set of all values associated
 * with a grouping and returns a single value that represents the average.
 */
public class Average extends Operation implements Aggregator
  {
  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;
  /** Field FIELD_NAME */
  private static final String FIELD_NAME = "average";
  /** Field cnt */
  private int cnt = 0;

  /** Constructor */
  public Average()
    {
    super( 1, new Fields( FIELD_NAME ) );
    }

  /** @see cascading.operation.Aggregator#start(Map) */
  @SuppressWarnings("unchecked")
  public void start( Map context )
    {
    context.put( FIELD_NAME, new Double( 0.0 ) );
    }

  /** @see cascading.operation.Aggregator#aggregate(Map, TupleEntry) */
  @SuppressWarnings("unchecked")
  public void aggregate( Map context, TupleEntry entry )
    {
    context.put( FIELD_NAME, ( (Double) context.get( FIELD_NAME ) + entry.getTuple().getDouble( 0 ) ) );
    cnt++;
    }

  /** @see cascading.operation.Aggregator#complete(Map, TupleEntryListIterator) */
  @SuppressWarnings("unchecked")
  public void complete( Map context, TupleEntryListIterator outputCollector )
    {
    // avoid Double.NaN
    if( cnt == 0 )
      outputCollector.add( new Tuple( new Double( 0.0 ) ) );
    else
      outputCollector.add( new Tuple( (Double) context.get( FIELD_NAME ) / cnt ) );
    }
  }
