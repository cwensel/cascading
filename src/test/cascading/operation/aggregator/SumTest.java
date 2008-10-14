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
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;

/** Test class for {@link Sum} */
public class SumTest extends CascadingTestCase
  {
  /** class under test */
  private Sum sum;
  private OperationCall operationCall;

  public SumTest()
    {
    super( "sum tests" );
    }

  public void setUp() throws Exception
    {
    sum = new Sum();
    operationCall = new OperationCall();
    }

  public void tearDown() throws Exception
    {
    sum = null;
    }

  public final void testSum()
    {
    assertEquals( "Got expected number of args", 1, sum.getNumArgs() );
    final Fields fields = new Fields( "sum" );
    assertEquals( "Got expected fields", fields, sum.getFieldDeclaration() );
    }

  public final void testSumFields()
    {
    final Fields fields = new Fields( "sum" );
    sum = new Sum( fields );
    assertEquals( "Got expected fields", fields, sum.getFieldDeclaration() );
    }

  public final void testStart()
    {
    sum.start( FlowProcess.NULL, operationCall );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    operationCall.setOutputCollector( resultEntryCollector );
    sum.complete( FlowProcess.NULL, operationCall );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected initial value on start", 0.0, tuple.getDouble( 0 ), 0.0d );
    }

  public final void testAggregateCompleteDouble()
    {
    sum.start( FlowProcess.NULL, operationCall );


    operationCall.setArguments( new TupleEntry( new Tuple( new Double( 1.0 ) ) ) );
    sum.aggregate( FlowProcess.NULL, operationCall );
    operationCall.setArguments( new TupleEntry( new Tuple( new Double( 3.0 ) ) ) );
    sum.aggregate( FlowProcess.NULL, operationCall );
    operationCall.setArguments( new TupleEntry( new Tuple( new Double( 2.0 ) ) ) );
    sum.aggregate( FlowProcess.NULL, operationCall );
    operationCall.setArguments( new TupleEntry( new Tuple( new Double( -4.0 ) ) ) );
    sum.aggregate( FlowProcess.NULL, operationCall );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    operationCall.setOutputCollector( resultEntryCollector );
    sum.complete( FlowProcess.NULL, operationCall );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected value after aggregate", 2.0, (Double) tuple.get( 0 ), 0.0d );
    }

  public final void testAggregateCompleteInteger()
    {
    Sum sum = new Sum( new Fields( "sum" ), Integer.class );
    sum.start( FlowProcess.NULL, operationCall );


    operationCall.setArguments( new TupleEntry( new Tuple( new Integer( 1 ) ) ) );
    sum.aggregate( FlowProcess.NULL, operationCall );
    operationCall.setArguments( new TupleEntry( new Tuple( new Integer( 3 ) ) ) );
    sum.aggregate( FlowProcess.NULL, operationCall );
    operationCall.setArguments( new TupleEntry( new Tuple( new Integer( 2 ) ) ) );
    sum.aggregate( FlowProcess.NULL, operationCall );
    operationCall.setArguments( new TupleEntry( new Tuple( new Integer( -4 ) ) ) );
    sum.aggregate( FlowProcess.NULL, operationCall );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    operationCall.setOutputCollector( resultEntryCollector );
    sum.complete( FlowProcess.NULL, operationCall );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected value after aggregate", 2, tuple.get( 0 ) );
    }
  }
