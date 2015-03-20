/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
