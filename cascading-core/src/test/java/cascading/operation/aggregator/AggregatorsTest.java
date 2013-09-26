/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.aggregator;

import java.util.Iterator;

import cascading.CascadingTestCase;
import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;

/**
 *
 */
public class AggregatorsTest extends CascadingTestCase
  {
  public AggregatorsTest()
    {
    }

  public void testAverage()
    {
    Aggregator aggregator = new Average();

    Tuple[] arguments = new Tuple[]{new Tuple( new Double( 1.0 ) ), new Tuple( new Double( 3.0 ) ),
                                    new Tuple( new Double( 2.0 ) ), new Tuple( new Double( 4.0 ) ),
                                    new Tuple( new Double( -5.0 ) )};

    Fields resultFields = new Fields( "field" );

    TupleListCollector resultEntryCollector = invokeAggregator( aggregator, arguments, resultFields );

    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "got expected value after aggregate", 1.0, tuple.getDouble( 0 ), 0.0d );
    }

  public void testCount()
    {
    Aggregator aggregator = new Count();

    Tuple[] arguments = new Tuple[]{new Tuple( new Double( 1.0 ) ), new Tuple( new Double( 3.0 ) ),
                                    new Tuple( new Double( 2.0 ) ), new Tuple( new Double( 4.0 ) ),
                                    new Tuple( new Double( -5.0 ) )};

    Fields resultFields = new Fields( "field" );

    TupleListCollector resultEntryCollector = invokeAggregator( aggregator, arguments, resultFields );

    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "got expected value after aggregate", 5, tuple.getInteger( 0 ) );
    }

  public void testFirst()
    {
    Aggregator aggregator = new First();

    Tuple[] arguments = new Tuple[]{new Tuple( new Double( 1.0 ) ), new Tuple( new Double( 3.0 ) ),
                                    new Tuple( new Double( 2.0 ) ), new Tuple( new Double( 4.0 ) ),
                                    new Tuple( new Double( -5.0 ) )};

    Fields resultFields = new Fields( "field" );

    TupleListCollector resultEntryCollector = invokeAggregator( aggregator, arguments, resultFields );

    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "got expected value after aggregate", 1.0, tuple.getDouble( 0 ), 0.0d );
    }

  public void testFirstN()
    {
    Aggregator aggregator = new First( 3 );

    Tuple[] arguments = new Tuple[]{new Tuple( new Double( 1.0 ) ), new Tuple( new Double( 3.0 ) ),
                                    new Tuple( new Double( 2.0 ) ), new Tuple( new Double( 4.0 ) ),
                                    new Tuple( new Double( -5.0 ) )};

    Fields resultFields = new Fields( "field" );

    TupleListCollector resultEntryCollector = invokeAggregator( aggregator, arguments, resultFields );

    Iterator<Tuple> iterator = resultEntryCollector.iterator();

    assertEquals( "got expected value after aggregate", 1.0, iterator.next().getDouble( 0 ), 0.0d );
    assertEquals( "got expected value after aggregate", 3.0, iterator.next().getDouble( 0 ), 0.0d );
    assertEquals( "got expected value after aggregate", 2.0, iterator.next().getDouble( 0 ), 0.0d );
    }

  public void testLast()
    {
    Aggregator aggregator = new Last();

    Tuple[] arguments = new Tuple[]{new Tuple( new Double( 1.0 ) ), new Tuple( new Double( 3.0 ) ),
                                    new Tuple( new Double( 2.0 ) ), new Tuple( new Double( 4.0 ) ),
                                    new Tuple( new Double( -5.0 ) )};

    Fields resultFields = new Fields( "field" );

    TupleListCollector resultEntryCollector = invokeAggregator( aggregator, arguments, resultFields );

    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "got expected value after aggregate", -5.0, tuple.getDouble( 0 ), 0.0d );
    }

  public void testMax()
    {
    Aggregator aggregator = new Max();

    Tuple[] arguments = new Tuple[]{new Tuple( new Double( 1.0 ) ), new Tuple( new Double( 3.0 ) ),
                                    new Tuple( new Double( 2.0 ) ), new Tuple( new Double( 4.0 ) ),
                                    new Tuple( new Double( -5.0 ) )};

    Fields resultFields = new Fields( "field" );

    TupleListCollector resultEntryCollector = invokeAggregator( aggregator, arguments, resultFields );

    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "got expected value after aggregate", 4.0, tuple.getDouble( 0 ), 0.0d );
    }

  public void testMin()
    {
    Aggregator aggregator = new Min();

    Tuple[] arguments = new Tuple[]{new Tuple( new Double( 1.0 ) ), new Tuple( new Double( 3.0 ) ),
                                    new Tuple( new Double( 2.0 ) ), new Tuple( new Double( 4.0 ) ),
                                    new Tuple( new Double( -5.0 ) )};

    Fields resultFields = new Fields( "field" );

    TupleListCollector resultEntryCollector = invokeAggregator( aggregator, arguments, resultFields );

    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "got expected value after aggregate", -5.0, tuple.getDouble( 0 ), 0.0d );
    }

  public void testSum()
    {
    Aggregator aggregator = new Sum();

    Tuple[] arguments = new Tuple[]{new Tuple( new Double( 1.0 ) ), new Tuple( new Double( 3.0 ) ),
                                    new Tuple( new Double( 2.0 ) ), new Tuple( new Double( 4.0 ) ),
                                    new Tuple( new Double( -5.0 ) )};

    Fields resultFields = new Fields( "field" );

    TupleListCollector resultEntryCollector = invokeAggregator( aggregator, arguments, resultFields );

    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "got expected value after aggregate", 5.0, tuple.getDouble( 0 ), 0.0d );
    }

  public void testMaxValue()
    {
    Aggregator aggregator = new MaxValue();

    Tuple[] arguments = new Tuple[]{new Tuple( new Double( 1.0 ) ), new Tuple( new Double( 3.0 ) ),
                                    new Tuple( new Double( 2.0 ) ), new Tuple( new Double( 4.0 ) ),
                                    new Tuple( new Double( -5.0 ) )};

    Fields resultFields = new Fields( "field" );

    TupleListCollector resultEntryCollector = invokeAggregator( aggregator, arguments, resultFields );

    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "got expected value after aggregate", 4.0, tuple.getDouble( 0 ), 0.0d );
    }

  public void testMinValue()
    {
    Aggregator aggregator = new MinValue();

    Tuple[] arguments = new Tuple[]{new Tuple( new Double( 1.0 ) ), new Tuple( new Double( 3.0 ) ),
                                    new Tuple( new Double( 2.0 ) ), new Tuple( new Double( 4.0 ) ),
                                    new Tuple( new Double( -5.0 ) )};

    Fields resultFields = new Fields( "field" );

    TupleListCollector resultEntryCollector = invokeAggregator( aggregator, arguments, resultFields );

    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "got expected value after aggregate", -5.0, tuple.getDouble( 0 ), 0.0d );
    }

  public void testMaxValueNonNumber()
    {
    Aggregator aggregator = new MaxValue();

    Tuple[] arguments = new Tuple[]{new Tuple( 'a' ), new Tuple( 'b' ),
                                    new Tuple( 'c' ), new Tuple( 'd' ),
                                    new Tuple( 'e' )};

    Fields resultFields = new Fields( "field" );

    TupleListCollector resultEntryCollector = invokeAggregator( aggregator, arguments, resultFields );

    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "got expected value after aggregate", 'e', tuple.getChar( 0 ) );
    }

  public void testMinValueNonNumber()
    {
    Aggregator aggregator = new MinValue();

    Tuple[] arguments = new Tuple[]{new Tuple( 'a' ), new Tuple( 'b' ),
                                    new Tuple( 'c' ), new Tuple( 'd' ),
                                    new Tuple( 'e' )};

    Fields resultFields = new Fields( "field" );

    TupleListCollector resultEntryCollector = invokeAggregator( aggregator, arguments, resultFields );

    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "got expected value after aggregate", 'a', tuple.getChar( 0 ) );
    }
  }
