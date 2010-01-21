/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

import java.io.File;
import java.io.IOException;

import cascading.ClusterTestCase;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
public class FlowProcessTest extends ClusterTestCase
  {

  String inputFileNums20 = "build/test/data/nums.20.txt";
  String inputFileNums10 = "build/test/data/nums.10.txt";

  String outputPath = "build/test/output/flowprocess/";

  public static class IterateInsert extends BaseOperation implements Function
    {
    private String path;

    public IterateInsert( Fields fieldDeclaration, String path )
      {
      super( fieldDeclaration );
      this.path = path;
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
        TupleEntryIterator iterator = flowProcess.openTapForRead( new Hfs( new TextLine( new Fields( "value" ) ), path ) );

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
    super( "flow process test", true );
    }

  public void testOpenForRead() throws IOException
    {
    if( !new File( inputFileNums20 ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileNums20 );
    copyFromLocal( inputFileNums10 );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileNums20 );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new IterateInsert( new Fields( "value" ), inputFileNums10 ), Fields.ALL );

    Tap sink = new Hfs( new TextLine(), outputPath + "/openforread", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "groupcount.dot" );

    flow.complete();

    validateLength( flow, 200, null );
    }
  }
