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

/** Class Count is an {@link Aggregator} that calculates the number of items in the current group */
public class Count extends Operation implements Aggregator
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
    super( 1, fieldDeclaration );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
    }

  /** @see Aggregator#start(Map, TupleEntry) */
  @SuppressWarnings("unchecked")
  public void start( Map context, TupleEntry groupEntry )
    {
    context.put( FIELD_NAME, 0L );
    }

  /** @see Aggregator#aggregate(Map, TupleEntry) */
  @SuppressWarnings("unchecked")
  public void aggregate( Map context, TupleEntry entry )
    {
    context.put( FIELD_NAME, (Long) context.get( FIELD_NAME ) + 1L );
    }

  /** @see Aggregator#complete(Map, TupleCollector) */
  @SuppressWarnings("unchecked")
  public void complete( Map context, TupleCollector outputCollector )
    {
    outputCollector.add( new Tuple( (Comparable) context.get( FIELD_NAME ) ) );
    }
  }
