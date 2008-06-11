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

package cascading.operation.assertion;

import java.util.Map;

import cascading.operation.GroupAssertion;
import cascading.tuple.TupleEntry;

/**
 *
 */
public abstract class AssertGroupBase extends AssertionBase implements GroupAssertion
  {
  /** Field COUNT */
  private static final String COUNT = "count";
  /** Field FIELDS */
  private static final String FIELDS = "fields";
  /** Field GROUP */
  private static final String GROUP = "group";
  /** Field size */
  protected long size;

  public AssertGroupBase( String message, long size )
    {
    super( message );
    this.size = size;
    }

  /** @see cascading.operation.Aggregator#start(java.util.Map, cascading.tuple.TupleEntry) */
  @SuppressWarnings("unchecked")
  public void start( Map context, TupleEntry groupEntry )
    {
    context.put( COUNT, 0L );
    context.put( FIELDS, groupEntry.getFields().print() );
    context.put( GROUP, groupEntry.getTuple().print() );
    }

  /** @see cascading.operation.Aggregator#aggregate(java.util.Map, cascading.tuple.TupleEntry) */
  @SuppressWarnings("unchecked")
  public void aggregate( Map context, TupleEntry entry )
    {
    context.put( COUNT, (Long) context.get( COUNT ) + 1L );
    }

  public void doAssert( Map context )
    {
    Long groupSize = (Long) context.get( COUNT );

    if( compare( groupSize ) )
      fail( groupSize, size, context.get( FIELDS ), context.get( GROUP ) );
    }

  protected abstract boolean compare( Long groupSize );
  }
