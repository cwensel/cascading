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

/** Test class for {@link Min} */
public class MinTest
  {

  /** class under test */
  private Min min;

  /** @throws java.lang.Exception  */
  @Before
  public void setUp() throws Exception
    {
    min = new Min();
    }

  /** @throws java.lang.Exception  */
  @After
  public void tearDown() throws Exception
    {
    min = null;
    }

  /** Test method for {@link cascading.operation.aggregator.Min#Min()}. */
  @Test
  public final void testMin()
    {
    assertEquals( "Got expected number of args", 1, min.getNumArgs() );
    final Fields fields = new Fields( "min" );
    assertEquals( "Got expected fields", fields, min.getFieldDeclaration() );
    }

  /** Test method for {@link cascading.operation.Aggregator#start(java.util.Map,cascading.tuple.TupleEntry)}. */
  @Test
  public final void testStart()
    {
    Map<String, Double> context = new HashMap<String, Double>();
    min.start( context, null );
    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    min.complete( context, resultEntryCollector );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected initial value on start", null, tuple.get( 0 ) );
    }

  /**
   * Test method for {@link cascading.operation.aggregator.Min#aggregate(java.util.Map, cascading.tuple.TupleEntry)}.
   * Test method for {@link cascading.operation.Aggregator#complete(java.util.Map,cascading.tuple.TupleCollector)}.
   */
  @Test
  public final void testAggregateComplete()
    {
    Map<String, Double> context = new HashMap<String, Double>();
    min.start( context, null );
    min.aggregate( context, new TupleEntry( new Tuple( new Double( 2.0 ) ) ) );
    min.aggregate( context, new TupleEntry( new Tuple( new Double( 1.0 ) ) ) );
    min.aggregate( context, new TupleEntry( new Tuple( new Double( 3.0 ) ) ) );
    min.aggregate( context, new TupleEntry( new Tuple( new Double( -4.0 ) ) ) );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    min.complete( context, resultEntryCollector );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected value after aggregate", -4.0, tuple.getDouble( 0 ), 0.0d );
    }

  }
