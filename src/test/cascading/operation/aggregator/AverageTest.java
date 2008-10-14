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

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.operation.ConcreteCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;

/** Test class for {@link Average} */
public class AverageTest extends CascadingTestCase
  {
  /** class under test */
  private Average average;
  private ConcreteCall operationCall;

  public AverageTest()
    {
    super( "average tests" );
    }

  public void setUp() throws Exception
    {
    average = new Average();
    operationCall = new ConcreteCall();
    }

  public void tearDown() throws Exception
    {
    average = null;
    }

  public final void testAverage()
    {
    assertEquals( "Got expected number of args", 1, average.getNumArgs() );
    final Fields fields = new Fields( "average" );
    assertEquals( "Got expected fields", fields, average.getFieldDeclaration() );
    }

  public final void testStart()
    {
    average.start( FlowProcess.NULL, operationCall );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    operationCall.setOutputCollector( resultEntryCollector );
    average.complete( FlowProcess.NULL, operationCall );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertTrue( "Got expected initial value on start", Double.isNaN( tuple.getDouble( 0 ) ) );
    }

  public final void testAggregateComplete()
    {
    average.start( FlowProcess.NULL, operationCall );

    operationCall.setArguments( new TupleEntry( new Tuple( new Double( 1.0 ) ) ) );
    average.aggregate( FlowProcess.NULL, operationCall );
    operationCall.setArguments( new TupleEntry( new Tuple( new Double( 3.0 ) ) ) );
    average.aggregate( FlowProcess.NULL, operationCall );
    operationCall.setArguments( new TupleEntry( new Tuple( new Double( 2.0 ) ) ) );
    average.aggregate( FlowProcess.NULL, operationCall );
    operationCall.setArguments( new TupleEntry( new Tuple( new Double( 4.0 ) ) ) );
    average.aggregate( FlowProcess.NULL, operationCall );
    operationCall.setArguments( new TupleEntry( new Tuple( new Double( -5.0 ) ) ) );
    average.aggregate( FlowProcess.NULL, operationCall );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    operationCall.setOutputCollector( resultEntryCollector );
    average.complete( FlowProcess.NULL, operationCall );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected value after aggregate", 1.0, tuple.getDouble( 0 ), 0.0d );
    }
  }
