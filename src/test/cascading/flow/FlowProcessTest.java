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

package cascading.flow;

import java.io.IOException;

import cascading.PlatformTestCase;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

import static data.InputData.inputFileNums10;
import static data.InputData.inputFileNums20;

/**
 *
 */
@PlatformTest(platforms = {"local", "hadoop"})
public class FlowProcessTest extends PlatformTestCase
  {
  public static class IterateInsert extends BaseOperation implements Function
    {
    private Tap tap;

    public IterateInsert( Fields fieldDeclaration, Tap tap )
      {
      super( fieldDeclaration );
      this.tap = tap;
      }

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall operationCall )
      {
      }

    @Override
    public void cleanup( FlowProcess flowProcess, OperationCall operationCall )
      {
      }

    @Override
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      try
        {
        TupleEntryIterator iterator = flowProcess.openTapForRead( tap );

        while( iterator.hasNext() )
          functionCall.getOutputCollector().add( new Tuple( iterator.next().getTuple() ) );

        iterator.close();
        }
      catch( IOException exception )
        {
        exception.printStackTrace();
        }
      }
    }

  public FlowProcessTest()
    {
    super( true );
    }

  public void testOpenForRead() throws IOException
    {
    getPlatform().copyFromLocal( inputFileNums20 );
    getPlatform().copyFromLocal( inputFileNums10 );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileNums20 );

    Pipe pipe = new Pipe( "test" );

    Tap tap = getPlatform().getTextFile( new Fields( "value" ), inputFileNums10 );

    pipe = new Each( pipe, new IterateInsert( new Fields( "value" ), tap ), Fields.ALL );

    Tap sink = getPlatform().getTextFile( getOutputPath( "openforread" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 200 );
    }
  }
