/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.partition;

import java.util.Iterator;

import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntrySchemeIterator;
import cascading.tuple.util.TupleViews;

/**
 *
 */
public class PartitionTupleEntryIterator implements Iterator<Tuple>
  {
  private final TupleEntrySchemeIterator childIterator;
  private final Tuple base;
  private final Tuple view;

  public PartitionTupleEntryIterator( Fields sourceFields, Partition partition, String parentIdentifier, String childIdentifier, TupleEntrySchemeIterator schemeIterator )
    {
    this.childIterator = schemeIterator;

    TupleEntry partitionEntry = new TupleEntry( partition.getPartitionFields(), Tuple.size( partition.getPartitionFields().size() ) );

    try
      {
      partition.toTuple( childIdentifier.substring( parentIdentifier.length() + 1 ), partitionEntry );
      }
    catch( Exception exception )
      {
      throw new TapException( "unable to parse partition given parent: " + parentIdentifier + " and child: " + childIdentifier );
      }

    base = TupleViews.createOverride( sourceFields, partitionEntry.getFields() );

    TupleViews.reset( base, Tuple.size( sourceFields.size() ), partitionEntry.getTuple() );

    view = TupleViews.createOverride( sourceFields, childIterator.getFields() );
    }

  @Override
  public boolean hasNext()
    {
    return childIterator.hasNext();
    }

  @Override
  public Tuple next()
    {
    Tuple tuple = childIterator.next().getTuple();
    TupleViews.reset( view, base, tuple );

    return view;
    }

  @Override
  public void remove()
    {
    childIterator.remove();
    }
  }
