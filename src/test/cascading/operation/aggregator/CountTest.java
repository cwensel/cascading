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

/** Test class for {@link Count} */
public class CountTest extends CascadingTestCase
  {
  /** class under test */
  private Count count;
  private OperationCall operationCall;

  public CountTest()
    {
    super( "count tests" );
    }

  public void setUp() throws Exception
    {
    count = new Count();
    operationCall = new OperationCall();
    }

  public void tearDown() throws Exception
    {
    count = null;
    }

  public final void testCount()
    {
    assertEquals( "Got expected number of args", Integer.MAX_VALUE, count.getNumArgs() );
    final Fields fields = new Fields( "count" );
    assertEquals( "Got expected fields", fields, count.getFieldDeclaration() );
    }

  public final void testStart()
    {
    count.start( FlowProcess.NULL, operationCall );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    operationCall.setOutputCollector( resultEntryCollector );

    count.complete( FlowProcess.NULL, operationCall );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected initial value on start", 0.0, tuple.getDouble( 0 ), 0.0d );
    }

  public final void testAggregateComplete()
    {
    count.start( FlowProcess.NULL, operationCall );
    operationCall.setArguments( new TupleEntry() );
    count.aggregate( FlowProcess.NULL, operationCall );
    operationCall.setArguments( new TupleEntry() );
    count.aggregate( FlowProcess.NULL, operationCall );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    operationCall.setOutputCollector( resultEntryCollector );
    count.complete( FlowProcess.NULL, operationCall );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected value after aggregate", 2, tuple.getDouble( 0 ), 0.0d );
    }

  }
