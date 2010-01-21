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

package cascading;

import java.io.File;
import java.util.Iterator;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class UnmodifiablePipesTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.10.txt";
  String inputFileIps = "build/test/data/ips.20.txt";
  String inputFileNums20 = "build/test/data/nums.20.txt";
  String inputFileNums10 = "build/test/data/nums.10.txt";
  String inputFileCritics = "build/test/data/critics.txt";

  String inputFileUpper = "build/test/data/upper.txt";
  String inputFileLower = "build/test/data/lower.txt";
  String inputFileLowerOffset = "build/test/data/lower-offset.txt";
  String inputFileJoined = "build/test/data/lower+upper.txt";

  String inputFileLhs = "build/test/data/lhs.txt";
  String inputFileRhs = "build/test/data/rhs.txt";
  String inputFileCross = "build/test/data/lhs+rhs-cross.txt";

  String outputPath = "build/test/output/unmodifiable/";

  public UnmodifiablePipesTest()
    {
    super( "buffer pipes", false ); // no need for clustering
    }

  public static class TestFunction extends BaseOperation implements Function
    {
    public TestFunction()
      {
      super( Fields.ARGS );
      }

    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      if( !functionCall.getArguments().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      functionCall.getOutputCollector().add( functionCall.getArguments() );
      }
    }

  public static class TestFilter extends BaseOperation implements Filter
    {
    public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
      {
      if( !filterCall.getArguments().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      return false;
      }
    }

  public static class TestAggregator extends BaseOperation implements Aggregator
    {
    public TestAggregator()
      {
      super( Fields.ARGS );
      }

    public void start( FlowProcess flowProcess, AggregatorCall aggregatorCall )
      {
      if( !aggregatorCall.getGroup().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );
      }

    public void aggregate( FlowProcess flowProcess, AggregatorCall aggregatorCall )
      {
      if( !aggregatorCall.getGroup().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      if( !aggregatorCall.getArguments().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );
      }

    public void complete( FlowProcess flowProcess, AggregatorCall aggregatorCall )
      {
      if( !aggregatorCall.getGroup().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      aggregatorCall.getOutputCollector().add( new Tuple( "some value" ) );
      }
    }

  public static class TestBuffer extends BaseOperation implements Buffer
    {
    public TestBuffer()
      {
      super( Fields.ARGS );
      }

    public void operate( FlowProcess flowProcess, BufferCall bufferCall )
      {
      if( !bufferCall.getGroup().isUnmodifiable() )
        throw new IllegalStateException( "is modifiable" );

      Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();

      while( iterator.hasNext() )
        {
        TupleEntry tupleEntry = iterator.next();

        if( !tupleEntry.isUnmodifiable() )
          throw new IllegalStateException( "is modifiable" );

        bufferCall.getOutputCollector().add( tupleEntry );
        }
      }
    }

  public void testUnmodifiable() throws Exception
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLhs );
    Tap sink = new Hfs( new TextLine(), outputPath + "/simple", true );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new TestFunction() );
    pipe = new Each( pipe, new Fields( "line" ), new TestFilter() );

    pipe = new GroupBy( pipe, new Fields( "line" ) );

    pipe = new Every( pipe, new TestAggregator(), Fields.RESULTS );
    pipe = new Each( pipe, new Fields( "line" ), new TestFunction() );

    pipe = new GroupBy( pipe, new Fields( "line" ) );
    pipe = new Every( pipe, new TestBuffer(), Fields.RESULTS );
    pipe = new Each( pipe, new Fields( "line" ), new TestFunction() );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "unknownselect.dot" );

    flow.complete();

    validateLength( flow, 13, null );
    }

  }