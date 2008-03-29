/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
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

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    average.complete( context, resultEntryCollector );
    Tuple tuple = resultEntryCollector.iterator().next();

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

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    average.complete( context, resultEntryCollector );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected value after aggregate", 1.0, tuple.getDouble( 0 ), 0.0d );
    }
  }
