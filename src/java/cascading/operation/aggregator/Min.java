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
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/** Class Min is an {@link Aggregator} that returns the minimum value encountered in the current group. */
public class Min extends Operation implements Aggregator
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "min";
  /** Field KEY_VALUE */
  private static final String KEY_VALUE = "value";

  /** Constructs a new instance that returns the min value encoutered in the field name "min". */
  public Min()
    {
    super( 1, new Fields( FIELD_NAME ) );
    }

  /**
   * Constructs a new instance that returns the min value encoutered in the given fieldDeclaration field name.
   *
   * @param fieldDeclaration of type Fields
   */
  public Min( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
    }

  /** @see Aggregator#start(Map, TupleEntry) */
  @SuppressWarnings("unchecked")
  public void start( Map context, TupleEntry groupEntry )
    {
    context.put( KEY_VALUE, Double.MAX_VALUE );
    }

  /** @see Aggregator#aggregate(Map, TupleEntry) */
  @SuppressWarnings("unchecked")
  public void aggregate( Map context, TupleEntry entry )
    {
    Number value = null;

    if( entry.getTuple().get( 0 ) instanceof Number )
      value = (Number) entry.getTuple().get( 0 );
    else
      value = entry.getTuple().getDouble( 0 );

    if( ( (Number) context.get( KEY_VALUE ) ).doubleValue() > value.doubleValue() )
      {
      context.put( FIELD_NAME, entry.getTuple().get( 0 ) ); // keep and return original value
      context.put( KEY_VALUE, value );
      }
    }

  /** @see Aggregator#complete(Map, TupleCollector) */
  @SuppressWarnings("unchecked")
  public void complete( Map context, TupleCollector outputCollector )
    {
    outputCollector.add( new Tuple( (Comparable) context.get( FIELD_NAME ) ) );
    }
  }
