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

package cascading.operation.aggregator;

import java.util.HashMap;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/** Test class for {@link Average} */
public class AverageTest
  {

  /** class under test */
  private Average average;

  /** @throws java.lang.Exception  */
  @Before
  public void setUp() throws Exception
    {
    average = new Average();
    }

  /** @throws java.lang.Exception  */
  @After
  public void tearDown() throws Exception
    {
    average = null;
    }

  /** Test method for {@link cascading.operation.aggregator.Average#Average()}. */
  @Test
  public final void testAverage()
    {
    assertEquals( "Got expected number of args", 1, average.getNumArgs() );
    final Fields fields = new Fields( "average" );
    assertEquals( "Got expected fields", fields, average.getFieldDeclaration() );
    }

  /** Test method for {@link cascading.operation.Aggregator#start(java.util.Map,cascading.tuple.TupleEntry)}. */
  @Test
  public final void testStart()
    {
    Map<String, Double> context = new HashMap<String, Double>();
    average.start( context, null );

    TupleEntryCollector resultEntryCollector = new TupleEntryCollector( new Fields( "field" ) );
    average.complete( context, resultEntryCollector.iterator() );
    Tuple tuple = resultEntryCollector.iterator().next().getTuple();

    assertEquals( "Got expected initial value on start", 0.0, tuple.getDouble( 0 ), 0.0d );
    }

  /**
   * Test method for {@link cascading.operation.aggregator.Average#aggregate(java.util.Map, cascading.tuple.TupleEntry)}.
   * Test method for {@link cascading.operation.Aggregator#complete(java.util.Map,cascading.tuple.TupleCollector)}.
   */
  @Test
  public final void testAggregateComplete()
    {
    Map<String, Double> context = new HashMap<String, Double>();
    average.start( context, null );
    average.aggregate( context, new TupleEntry( new Tuple( new Double( 1.0 ) ) ) );
    average.aggregate( context, new TupleEntry( new Tuple( new Double( 3.0 ) ) ) );
    average.aggregate( context, new TupleEntry( new Tuple( new Double( 2.0 ) ) ) );
    average.aggregate( context, new TupleEntry( new Tuple( new Double( 4.0 ) ) ) );
    average.aggregate( context, new TupleEntry( new Tuple( new Double( -5.0 ) ) ) );

    TupleEntryCollector resultEntryCollector = new TupleEntryCollector( new Fields( "field" ) );
    average.complete( context, resultEntryCollector.iterator() );
    Tuple tuple = resultEntryCollector.iterator().next().getTuple();

    assertEquals( "Got expected value after aggregate", 1.0, tuple.getDouble( 0 ), 0.0d );
    }
  }
