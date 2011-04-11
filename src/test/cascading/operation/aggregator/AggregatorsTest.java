/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import cascading.CascadingTestCase;
import cascading.operation.Aggregator;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;

/**
 *
 */
@PlatformTest(platforms = {"none"})
public class AggregatorsTest extends CascadingTestCase
  {
  public AggregatorsTest()
    {
    super();
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

  }
