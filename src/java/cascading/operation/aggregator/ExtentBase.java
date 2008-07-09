/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import cascading.operation.Aggregator;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/** Class ExtentBase is the base class for First and Last. */
public abstract class ExtentBase extends BaseOperation implements Aggregator
  {
  /** Field defaultFieldName */
  private final String defaultFieldName;
  /** Field ignoreTuples */
  private final Collection<Tuple> ignoreTuples;

  protected ExtentBase( Fields fieldDeclaration, String defaultFieldName )
    {
    super( fieldDeclaration );
    this.defaultFieldName = defaultFieldName;
    this.ignoreTuples = null;
    }

  protected ExtentBase( int numArgs, Fields fieldDeclaration, String defaultFieldName )
    {
    super( numArgs, fieldDeclaration );
    this.defaultFieldName = defaultFieldName;
    ignoreTuples = null;
    }

  protected ExtentBase( Fields fieldDeclaration, String defaultFieldName, Tuple... ignoreTuples )
    {
    super( fieldDeclaration );
    this.defaultFieldName = defaultFieldName;
    this.ignoreTuples = new HashSet<Tuple>();
    Collections.addAll( this.ignoreTuples, ignoreTuples );
    }

  @SuppressWarnings("unchecked")
  public void start( Map context, TupleEntry groupEntry )
    {
    // no-op
    }

  /** @see cascading.operation.Aggregator#aggregate(java.util.Map, cascading.tuple.TupleEntry) */
  @SuppressWarnings("unchecked")
  public void aggregate( Map context, TupleEntry entry )
    {
    if( ignoreTuples != null && ignoreTuples.contains( entry.getTuple() ) )
      return;

    performOperation( context, entry );
    }

  protected abstract void performOperation( Map context, TupleEntry entry );

  /** @see cascading.operation.Aggregator#complete(java.util.Map, cascading.tuple.TupleCollector) */
  @SuppressWarnings("unchecked")
  public void complete( Map context, TupleCollector outputCollector )
    {
    if( context.containsKey( defaultFieldName ) )
      outputCollector.add( (Tuple) context.get( defaultFieldName ) );
    }
  }
