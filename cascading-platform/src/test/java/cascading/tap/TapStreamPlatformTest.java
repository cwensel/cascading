/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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

package cascading.tap;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryStream;
import cascading.tuple.TupleStream;
import cascading.tuple.coerce.Coercions;
import org.junit.Test;

import static data.InputData.inputFileNums20;

/**
 *
 */
public class TapStreamPlatformTest extends PlatformTestCase
  {
  public TapStreamPlatformTest()
    {
    super( false );
    }

  @Test
  public void testFlowStream() throws Exception
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", inputFileNums20 );

    Pipe pipe = new Pipe( "test" );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), ",", getOutputPath(), SinkMode.REPLACE );

    Flow<?> flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    int sum = flow.getSinkEntryStream()
      .mapToInt( TupleEntryStream.fieldToInt( Fields.FIRST ) )
      .sum();

    assertEquals( 210, sum );
    }

  @Test
  public void testTupleEntryStream() throws Exception
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    FlowProcess flowProcess = getPlatform().getFlowProcess();
    Tap tap = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", inputFileNums20 );

    long count = TupleEntryStream.entryStream( tap, flowProcess ).count();

    assertEquals( 20, count );

    int sum = TupleEntryStream.entryStream( tap, flowProcess )
      .mapToInt( TupleEntryStream.fieldToInt( Fields.FIRST ) )
      .sum();

    assertEquals( 210, sum );

    List<Object> collect1 = TupleEntryStream.entryStream( tap, flowProcess )
      .map( TupleEntryStream.fieldToObject( Fields.FIRST ) )
      .collect( Collectors.toList() );

    assertEquals( 20, collect1.size() );

    List<Integer> collect2 = TupleEntryStream.entryStream( tap, flowProcess )
      .map( TupleEntryStream.fieldToObject( Fields.FIRST, Integer.class ) )
      .collect( Collectors.toList() );

    assertEquals( 20, collect2.size() );

    List<Integer> collect3 = TupleEntryStream.entryStream( tap, flowProcess )
      .map( TupleEntryStream.fieldToObject( Fields.FIRST, Coercions.INTEGER_OBJECT ) ) // CoercibleType<Integer>
      .collect( Collectors.toList() );

    assertEquals( 20, collect3.size() );

    // same instance
    Set<TupleEntry> collect4 = TupleEntryStream.entryStream( tap, flowProcess )
      .collect( Collectors.toSet() ); // error prone, instances are re-used

    assertEquals( 1, new HashSet<>( collect4 ).size() ); // re-hash

    // new instance
    Set<TupleEntry> collect5 = TupleEntryStream.entryStreamCopy( tap, flowProcess )
      .collect( Collectors.toSet() ); // correct

    assertEquals( 20, new HashSet<>( collect5 ).size() ); // re-hash
    }

  @Test
  public void testTupleStream() throws Exception
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    FlowProcess flowProcess = getPlatform().getFlowProcess();
    Tap tap = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", inputFileNums20 );

    long count = TupleStream.tupleStream( tap, flowProcess ).count();

    assertEquals( 20, count );

    int sum = TupleStream.tupleStream( tap, flowProcess )
      .mapToInt( TupleStream.posToInt( 0 ) )
      .sum();

    assertEquals( 210, sum );

    List<Object> collect1 = TupleStream.tupleStream( tap, flowProcess )
      .map( TupleStream.posToObject( 0 ) )
      .collect( Collectors.toList() );

    assertEquals( 20, collect1.size() );

    // same instance
    Set<Tuple> collect2 = TupleStream.tupleStream( tap, flowProcess )
      .collect( Collectors.toSet() ); // error prone, instances are re-used

    assertEquals( 1, new HashSet<>( collect2 ).size() ); // re-hash

    // new instance
    Set<Tuple> collect3 = TupleStream.tupleStreamCopy( tap, flowProcess )
      .collect( Collectors.toSet() ); // correct

    assertEquals( 20, new HashSet<>( collect3 ).size() ); // re-hash
    }

  @Test
  public void testTupleEntryWriter() throws Exception
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    FlowProcess flowProcess = getPlatform().getFlowProcess();
    Tap source = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", inputFileNums20 );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", getOutputPath() );

    Stream<TupleEntry> stream = TupleEntryStream.entryStream( source, flowProcess );

    Tap result = TupleEntryStream.writeEntry( stream::iterator, sink, flowProcess );

    assertEquals( 20, TupleEntryStream.entryStream( result, flowProcess ).count() );
    }

  @Test
  public void testTupleEntryTupleWriter() throws Exception
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    FlowProcess flowProcess = getPlatform().getFlowProcess();
    Tap source = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", inputFileNums20 );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", getOutputPath() );

    Stream<Tuple> stream = TupleEntryStream.entryStream( source, flowProcess )
      .map( entry -> entry.selectTuple( new Fields( "num" ) ) );

    Tap result = TupleStream.writeTuple( stream, sink, flowProcess );

    assertEquals( 20, TupleEntryStream.entryStream( result, flowProcess ).count() );
    }

  @Test
  public void testTupleEntryIntWriter() throws Exception
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    FlowProcess flowProcess = getPlatform().getFlowProcess();
    Tap source = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", inputFileNums20 );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", getOutputPath() );

    IntStream stream = TupleEntryStream.entryStream( source, flowProcess )
      .mapToInt( TupleEntryStream.fieldToInt( new Fields( "num" ) ) );

    Tap result = TupleStream.writeInt( stream, sink, flowProcess );

    assertEquals( 20, TupleEntryStream.entryStream( result, flowProcess ).count() );
    }

  @Test
  public void testTupleEntryLongWriter() throws Exception
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    FlowProcess flowProcess = getPlatform().getFlowProcess();
    Tap source = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", inputFileNums20 );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", getOutputPath() );

    LongStream stream = TupleEntryStream.entryStream( source, flowProcess )
      .mapToLong( TupleEntryStream.fieldToLong( new Fields( "num" ) ) );

    Tap result = TupleStream.writeLong( stream, sink, flowProcess );

    assertEquals( 20, TupleEntryStream.entryStream( result, flowProcess ).count() );
    }

  @Test
  public void testTupleEntryDoubleWriter() throws Exception
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    FlowProcess flowProcess = getPlatform().getFlowProcess();
    Tap source = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", inputFileNums20 );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "num", Integer.class ), " ", getOutputPath() );

    DoubleStream stream = TupleEntryStream.entryStream( source, flowProcess )
      .mapToDouble( TupleEntryStream.fieldToDouble( new Fields( "num" ) ) );

    Tap result = TupleStream.writeDouble( stream, sink, flowProcess );

    assertEquals( 20, TupleEntryStream.entryStream( result, flowProcess ).count() );
    }
  }
