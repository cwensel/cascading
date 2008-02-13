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

/** Class First ... */
public class First extends Operation implements Aggregator
  {
  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;
  /** Field FIELD_NAME */
  private static final String FIELD_NAME = "first";

  /** Constructor */
  public First()
    {
    super( new Fields( FIELD_NAME ) );
    }

  /**
   * Constructor
   *
   * @param fields the fields to operate on
   */
  public First( Fields fields )
    {
    super( fields );
    }

  @SuppressWarnings("unchecked")
  public void start( Map context, TupleEntry groupEntry )
    {
    // no-op
    }

  /** @see Aggregator#aggregate(Map, TupleEntry) */
  @SuppressWarnings("unchecked")
  public void aggregate( Map context, TupleEntry entry )
    {
    if( !context.containsKey( FIELD_NAME ) )
      context.put( FIELD_NAME, entry.getTuple() );
    }

  /** @see Aggregator#complete(java.util.Map,cascading.tuple.TupleCollector) */
  @SuppressWarnings("unchecked")
  public void complete( Map context, TupleCollector outputCollector )
    {
    if( context.containsKey( FIELD_NAME ) )
      outputCollector.add( (Tuple) context.get( FIELD_NAME ) );
    }
  }
