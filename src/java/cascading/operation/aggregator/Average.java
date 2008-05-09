/*
 * Copyright (c) 2007-2008 Chris K Wensel. All Rights Reserved.
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
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/** Class Average is an {@link Aggregator} that returns the average of all numeric values in the current group. */
public class Average extends Operation implements Aggregator
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "average";
  /** Field KEY_COUNT */
  private static final String KEY_COUNT = "count";

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
  public Average( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
    }

  /** @see Aggregator#start(Map, TupleEntry) */
  @SuppressWarnings("unchecked")
  public void start( Map context, TupleEntry groupEntry )
    {
    context.put( FIELD_NAME, 0.0d );
    context.put( KEY_COUNT, 0L );
    }

  /** @see Aggregator#aggregate(Map, TupleEntry) */
  @SuppressWarnings("unchecked")
  public void aggregate( Map context, TupleEntry entry )
    {
    context.put( FIELD_NAME, (Double) context.get( FIELD_NAME ) + entry.getTuple().getDouble( 0 ) );
    context.put( KEY_COUNT, (Long) context.get( KEY_COUNT ) + 1 );
    }

  /** @see Aggregator#complete(Map, TupleCollector) */
  @SuppressWarnings("unchecked")
  public void complete( Map context, TupleCollector outputCollector )
    {
    long count = (Long) context.get( KEY_COUNT );

    outputCollector.add( new Tuple( (Double) context.get( FIELD_NAME ) / count ) );
    }
  }
