/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import java.beans.ConstructorProperties;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class First is an {@link Aggregator} that returns the first {@link Tuple} encountered.
 * <p/>
 * By default, it returns the first Tuple of {@link Fields#ARGS} found.
 */
public class First extends ExtentBase
  {
  /** Selects and returns the first argument Tuple encountered. */
  public First()
    {
    super( Fields.ARGS );
    }

  /**
   * Selects and returns the first argument Tuple encountered.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public First( Fields fieldDeclaration )
    {
    super( fieldDeclaration.size(), fieldDeclaration );
    }

  /**
   * Selects and returns the first argument Tuple encountered, unless the Tuple
   * is a member of the set ignoreTuples.
   *
   * @param fieldDeclaration of type Fields
   * @param ignoreTuples     of type Tuple...
   */
  @ConstructorProperties({"fieldDeclaration", "ignoreTuples"})
  public First( Fields fieldDeclaration, Tuple... ignoreTuples )
    {
    super( fieldDeclaration, ignoreTuples );
    }

  protected void performOperation( Tuple[] context, TupleEntry entry )
    {
    if( context[ 0 ] == null )
      context[ 0 ] = new Tuple( entry.getTuple() );
    }

  }
