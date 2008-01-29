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

/** Class Sum ... */
public class Sum extends Operation implements Aggregator
  {
  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;
  /** Field FIELD_NAME */
  private static final String FIELD_NAME = "sum";

  /**
   * Constructor Sum creates a new Sum instance that accepts one argument and returns a single field named "sum".
   */
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
  public Sum( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );
    }

  /** @see Aggregator#start(Map) */
  @SuppressWarnings("unchecked")
  public void start( Map context )
    {
    context.put( FIELD_NAME, 0.0d );
    }

  /** @see Aggregator#aggregate(Map, TupleEntry) */
  @SuppressWarnings("unchecked")
  public void aggregate( Map context, TupleEntry entry )
    {
    context.put( FIELD_NAME, ( (Double) context.get( FIELD_NAME ) ) + entry.getTuple().getDouble( 0 ) );
    }

  /** @see Aggregator#complete(Map, TupleEntryListIterator) */
  @SuppressWarnings("unchecked")
  public void complete( Map context, TupleEntryListIterator outputCollector )
    {
    outputCollector.add( new Tuple( (Double) context.get( FIELD_NAME ) ) );
    }
  }
