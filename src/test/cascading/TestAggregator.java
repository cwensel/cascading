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

package cascading;

import java.util.Map;

import cascading.operation.Aggregator;
import cascading.operation.Operation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryListIterator;

/** @version $Id: //depot/calku/cascading/src/test/cascading/TestAggregator.java#2 $ */
public class TestAggregator extends Operation implements Aggregator
  {
  private static final long serialVersionUID = 1L;
  private Tuple value;

  /**
   * Constructor
   *
   * @param fields the fields to operate on
   */
  public TestAggregator( Fields fields, Tuple value )
    {
    super( fields );
    this.value = value;
    }

  /** @see cascading.operation.Aggregator#start(java.util.Map) */
  @SuppressWarnings("unchecked")
  public void start( Map context )
    {
    // no-op
    }

  /** @see cascading.operation.Aggregator#aggregate(java.util.Map, cascading.tuple.TupleEntry) */
  @SuppressWarnings("unchecked")
  public void aggregate( Map context, TupleEntry entry )
    {
    }

  /** @see cascading.operation.Aggregator#complete(java.util.Map, cascading.tuple.TupleEntryListIterator) */
  @SuppressWarnings("unchecked")
  public void complete( Map context, TupleEntryListIterator outputCollector )
    {
    outputCollector.add( value );
    }
  }