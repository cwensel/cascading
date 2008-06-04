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

import java.util.HashMap;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/** Test class for {@link Count} */
public class CountTest
  {

  /** class under test */
  private Count count;

  /** @throws java.lang.Exception  */
  @Before
  public void setUp() throws Exception
    {
    count = new Count();
    }

  /** @throws java.lang.Exception  */
  @After
  public void tearDown() throws Exception
    {
    count = null;
    }

  /** Test method for {@link cascading.operation.aggregator.Count#Count()}. */
  @Test
  public final void testCount()
    {
    assertEquals( "Got expected number of args", Integer.MAX_VALUE, count.getNumArgs() );
    final Fields fields = new Fields( "count" );
    assertEquals( "Got expected fields", fields, count.getFieldDeclaration() );
    }

  /** Test method for {@link cascading.operation.Aggregator#start(java.util.Map,cascading.tuple.TupleEntry)}. */
  @Test
  public final void testStart()
    {
    Map<String, Double> context = new HashMap<String, Double>();
    count.start( context, null );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    count.complete( context, resultEntryCollector );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected initial value on start", 0.0, tuple.getDouble( 0 ), 0.0d );
    }

  /**
   * Test method for {@link cascading.operation.aggregator.Count#aggregate(java.util.Map, cascading.tuple.TupleEntry)}.
   * Test method for {@link cascading.operation.Aggregator#complete(java.util.Map,cascading.tuple.TupleCollector)}.
   */
  @Test
  public final void testAggregateComplete()
    {
    Map<String, Integer> context = new HashMap<String, Integer>();
    count.start( context, null );
    count.aggregate( context, new TupleEntry() );
    count.aggregate( context, new TupleEntry() );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    count.complete( context, resultEntryCollector );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected value after aggregate", 2, tuple.getDouble( 0 ), 0.0d );
    }

  }
