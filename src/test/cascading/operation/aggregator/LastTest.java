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
import cascading.flow.FlowSession;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;

/** Test class for {@link cascading.operation.aggregator.First} */
public class LastTest extends CascadingTestCase
  {
  /** class under test */
  private Last last;
  private OperationCall operationCall;

  public LastTest()
    {
    super( "last tests" );
    }

  public void setUp() throws Exception
    {
    last = new Last();
    operationCall = new OperationCall();
    }

  public void tearDown() throws Exception
    {
    last = null;
    }

  public final void testFirst()
    {
    assertEquals( "Got expected number of args", Integer.MAX_VALUE, last.getNumArgs() );
    assertEquals( "Got expected fields", Fields.ARGS, last.getFieldDeclaration() );
    }

  public final void testStart()
    {
    last.start( FlowSession.NULL, operationCall );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    operationCall.setOutputCollector( resultEntryCollector );
    last.complete( FlowSession.NULL, operationCall );

    assertEquals( "Got expected initial value on start", false, resultEntryCollector.iterator().hasNext() );
    }

  public final void testAggregateComplete()
    {
    last.start( FlowSession.NULL, operationCall );


    operationCall.setArguments( new TupleEntry( new Tuple( new Double( 0.0 ) ) ) );
    last.aggregate( FlowSession.NULL, operationCall );

    operationCall.setArguments( new TupleEntry( new Tuple( new Double( 1.0 ) ) ) );
    last.aggregate( FlowSession.NULL, operationCall );

    TupleListCollector resultEntryCollector = new TupleListCollector( new Fields( "field" ) );
    operationCall.setOutputCollector( resultEntryCollector );
    last.complete( FlowSession.NULL, operationCall );
    Tuple tuple = resultEntryCollector.iterator().next();

    assertEquals( "Got expected value after aggregate", 1.0, tuple.getDouble( 0 ), 0.0d );
    }
  }