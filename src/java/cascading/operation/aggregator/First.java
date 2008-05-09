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
  /** Field FIELD_NAME */
  private static final String FIELD_NAME = "first";

  /** Selects and returns the first argument Tuple encountered. */
  public First()
    {
    super( Fields.ARGS, FIELD_NAME );
    }

  /**
   * Selects and returns the first argument Tuple encountered.
   *
   * @param fieldDeclaration of type Fields
   */
  public First( Fields fieldDeclaration )
    {
    super( fieldDeclaration.size(), fieldDeclaration, FIELD_NAME );
    }

  /**
   * Selects and returns the first argument Tuple encountered, unless the Tuple
   * is a member of the set ignoreTuples.
   *
   * @param fieldDeclaration of type Fields
   * @param ignoreTuples     of type Tuple...
   */
  public First( Fields fieldDeclaration, Tuple... ignoreTuples )
    {
    super( fieldDeclaration, FIELD_NAME, ignoreTuples );
    }

  @SuppressWarnings("unchecked")
  protected void performOperation( Map context, TupleEntry entry )
    {
    if( !context.containsKey( FIELD_NAME ) )
      context.put( FIELD_NAME, entry.getTuple() );
    }

  }
