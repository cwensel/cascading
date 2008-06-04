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

/** Test class for {@link cascading.operation.aggregator.First} */
public class LastTest
  {

  /** class under test */
  private Last last;

  /** @throws Exception  */
  @Before
  public void setUp() throws Exception
    {
    last = new Last();
    }

  /** @throws Exception  */
  @After
  public void tearDown() throws Exception
    {
    last = null;
    }

  /** Test method for {@link First#First()}. */
  @Test
  public final void testFirst()
    {
    assertEquals( "Got expected number of args", Integer.MAX_VALUE, last.getNumArgs() );
    assertEquals( "Got expected fields", Fields.ARGS, last.getFieldDeclaration() );
    }

  /** Test method for {@link cascading.operation.Aggregator#start(java.util.Map,cascading.tuple.TupleEntry)}. */
  @Test
  public final void testStart()
    {
    Map<String, Double> context = new HashMap<String, Double>();
    last.start( context, null );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    last.complete( context, resultEntryCollector );

    assertEquals( "Got expected initial value on start", false, resultEntryCollector.iterator().hasNext() );
    }

  /**
   * Test method for {@link First#aggregate(java.util.Map, cascading.tuple.TupleEntry)}.
   * Test method for {@link cascading.operation.Aggregator#complete(java.util.Map,cascading.tuple.TupleCollector)}.
   */
  @Test
  public final void testAggregateComplete()
    {
    Map<String, Double> context = new HashMap<String, Double>();
    last.start( context, null );
    last.aggregate( context, new TupleEntry( new Tuple( new Double( 0.0 ) ) ) );
    last.aggregate( context, new TupleEntry( new Tuple( new Double( 1.0 ) ) ) );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    last.complete( context, resultEntryCollector );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected value after aggregate", 1.0, tuple.getDouble( 0 ), 0.0d );
    }
  }