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
import cascading.operation.Operation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/**
 * Class ExtremaBase is the base class for Max and Min. The unique thing about Max and Min are that they return the original,
 * un-coerced, argument value, though a coerced version of the argument is used for the comparison.
 */
public abstract class ExtremaBase extends Operation implements Aggregator
  {
  /** Field FIELD_NAME */
  private static final String KEY_EXTREMA = "extrema";
  /** Field KEY_VALUE */
  private static final String KEY_VALUE = "value";
  /** Field ignoreValues */
  protected final Collection ignoreValues;

  public ExtremaBase( Fields fieldDeclaration )
    {
    super( fieldDeclaration );
    ignoreValues = null;
    }

  public ExtremaBase( int numArgs, Fields fieldDeclaration )
    {
    super( numArgs, fieldDeclaration );
    ignoreValues = null;

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
    }

  protected ExtremaBase( Fields fieldDeclaration, Object... ignoreValues )
    {
    super( fieldDeclaration );
    this.ignoreValues = new HashSet();
    Collections.addAll( this.ignoreValues, ignoreValues );
    }

  /** @see cascading.operation.Aggregator#start(java.util.Map , cascading.tuple.TupleEntry) */
  @SuppressWarnings("unchecked")
  public void start( Map context, TupleEntry groupEntry )
    {
    context.put( KEY_VALUE, getInitialValue() );
    }

  protected abstract double getInitialValue();

  /** @see cascading.operation.Aggregator#aggregate(java.util.Map, cascading.tuple.TupleEntry) */
  @SuppressWarnings("unchecked")
  public void aggregate( Map context, TupleEntry entry )
    {
    Comparable arg = entry.getTuple().get( 0 );

    if( ignoreValues != null && ignoreValues.contains( arg ) )
      return;

    Number rhs = null;

    if( arg instanceof Number )
      rhs = (Number) arg;
    else
      rhs = entry.getTuple().getDouble( 0 );

    Number lhs = (Number) context.get( KEY_VALUE );

    if( compare( lhs, rhs ) )
      {
      context.put( KEY_EXTREMA, arg ); // keep and return original value
      context.put( KEY_VALUE, rhs );
      }
    }

  protected abstract boolean compare( Number lhs, Number rhs );

  /** @see cascading.operation.Aggregator#complete(java.util.Map, cascading.tuple.TupleCollector) */
  @SuppressWarnings("unchecked")
  public void complete( Map context, TupleCollector outputCollector )
    {
    outputCollector.add( new Tuple( (Comparable) context.get( KEY_EXTREMA ) ) );
    }
  }
