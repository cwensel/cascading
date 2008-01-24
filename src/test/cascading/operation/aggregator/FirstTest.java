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

/** Test class for {@link First} */
public class FirstTest
  {

  /** class under test */
  private First first;

  /** @throws java.lang.Exception  */
  @Before
  public void setUp() throws Exception
    {
    first = new First();
    }

  /** @throws java.lang.Exception  */
  @After
  public void tearDown() throws Exception
    {
    first = null;
    }

  /** Test method for {@link cascading.operation.aggregator.First#First()}. */
  @Test
  public final void testFirst()
    {
    assertEquals( "Got expected number of args", Integer.MAX_VALUE, first.getNumArgs() );
    final Fields fields = new Fields( "first" );
    assertEquals( "Got expected fields", fields, first.getFieldDeclaration() );
    }

  /** Test method for {@link cascading.operation.aggregator.First#First(cascading.tuple.Fields)}. */
  @Test
  public final void testFirstFields()
    {
    final Fields fields = new Fields( "first", "col1", "col2" );
    first = new First( fields );
    assertEquals( "Got expected fields ", fields, first.getFieldDeclaration() );
    }

  /** Test method for {@link cascading.operation.aggregator.First#start(java.util.Map)}. */
  @Test
  public final void testStart()
    {
    Map<String, Double> context = new HashMap<String, Double>();
    first.start( context );

    TupleEntryCollector resultEntryCollector = new TupleEntryCollector( new Fields( "field" ) );
    first.complete( context, resultEntryCollector.iterator() );

    assertEquals( "Got expected initial value on start", false, resultEntryCollector.iterator().hasNext() );
    }

  /**
   * Test method for {@link cascading.operation.aggregator.First#aggregate(java.util.Map, cascading.tuple.TupleEntry)}.
   * Test method for {@link cascading.operation.Aggregator#complete(java.util.Map, cascading.tuple.TupleEntryListIterator)}.
   */
  @Test
  public final void testAggregateComplete()
    {
    Map<String, Double> context = new HashMap<String, Double>();
    first.start( context );
    first.aggregate( context, new TupleEntry( new Tuple( new Double( 1.0 ) ) ) );
    first.aggregate( context, new TupleEntry( new Tuple( new Double( 0.0 ) ) ) );

    TupleEntryCollector resultEntryCollector = new TupleEntryCollector( new Fields( "field" ) );
    first.complete( context, resultEntryCollector.iterator() );
    Tuple tuple = resultEntryCollector.iterator().next().getTuple();

    assertEquals( "Got expected value after aggregate", 1.0, tuple.getDouble( 0 ), 0.0d );
    }
  }
