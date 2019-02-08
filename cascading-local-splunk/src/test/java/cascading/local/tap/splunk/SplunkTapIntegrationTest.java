/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.local.tap.splunk;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.splunk.Index;
import com.splunk.SDKTestCase;
import com.splunk.Service;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class SplunkTapIntegrationTest extends SDKTestCase
  {
  public static final int PORT = 8000;

  @ClassRule
  public static GenericContainer splunk = new GenericContainer( "splunk/splunk:7.2.0" )
    .withExposedPorts( PORT )
    .withEnv( "SPLUNK_START_ARGS", "--accept-license" )
    .withEnv( "SPLUNK_PASSWORD", "helloworld" )
    .withEnv( "NO_HEALTHCHECK", "true" )
    .withStartupTimeout( Duration.ofMinutes( 3 ) );

  private String indexName;
  private Index index;

  @Before
  @Override
  public void setUp() throws Exception
    {
    Map<String, Object> args = new HashMap<>();

    args.put( "host", splunk.getContainerIpAddress() );
    args.put( "port", splunk.getMappedPort( 8089 ) );
    args.put( "username", "admin" );
    args.put( "password", "helloworld" );

    service = new Service( args );

    super.setUp();

    indexName = createTemporaryName();
    index = service.getIndexes().create( indexName );

    assertEventuallyTrue( () -> service.getIndexes().containsKey( indexName ) );
    }

  @After
  @Override
  public void tearDown() throws Exception
    {
    if( service != null && service.getIndexes().containsKey( indexName ) )
      index.remove();

    super.tearDown();
    }

  @Test
  public void writeDelimitedReadCSV() throws IOException
    {
    Fields sinkFields = new Fields( "time", long.class ).append( new Fields( "value", int.class ) );
    Fields sourceFields = SplunkCSV.DEFAULTS
      .append( SplunkCSV._INDEXTIME )
      .append( SplunkCSV._SUBSECOND )
      .append( SplunkCSV.TIMESTARTPOS )
      .append( SplunkCSV.TIMEENDPOS );

    writeRead(
      () -> new SplunkIndexTap( new SplunkRawDelimited( sinkFields ), service, indexName ),
      () -> new SplunkIndexTap( new SplunkCSV( sourceFields ), service, indexName ),
      ( count, next ) -> count + 1,
      1, 100
    );
    }

  @Test
  public void writeDelimitedReadCSVNarrow() throws IOException
    {
    writeRead( () ->
      {
      SplunkRawDelimited sink = new SplunkRawDelimited( new Fields( "time", long.class ).append( new Fields( "value", int.class ) ) );
      return new SplunkIndexTap( sink, service, indexName );
      }, () ->
      {
      SplunkCSV source = new SplunkCSV( SplunkCSV._TIME.append( SplunkCSV._RAW ) );
      return new SplunkIndexTap( source, service, indexName );
      },
      ( count, next ) -> count + 1,
      1, 100
    );
    }

  @Test
  public void writeDelimitedReadCSVAll() throws IOException
    {
    writeRead( () ->
      {
      SplunkRawDelimited sink = new SplunkRawDelimited( new Fields( "time", long.class ).append( new Fields( "value", int.class ) ) );
      return new SplunkIndexTap( sink, service, indexName );
      }, () ->
      {
      SplunkCSV source = new SplunkCSV( Fields.ALL );
      return new SplunkIndexTap( source, service, indexName );
      },
      ( count, next ) -> count + 1,
      1, 100
    );
    }

  @Test
  public void writeDelimitedReadCSVAllMulti() throws IOException
    {
    writeRead( () ->
      {
      SplunkRawDelimited sink = new SplunkRawDelimited( new Fields( "time", long.class ).append( new Fields( "value", int.class ) ) );
      return new SplunkIndexTap( sink, service, indexName );
      }, () ->
      {
      SplunkCSV source = new SplunkCSV( Fields.ALL );
      return new SplunkIndexTap( source, service, indexName );
      },
      ( count, next ) -> count + 1,
      3, 99
    );
    }

  @Test
  public void writeDelimitedReadDelimited() throws IOException
    {
    SplunkRawDelimited scheme = new SplunkRawDelimited( new Fields( "time", long.class ).append( new Fields( "value", int.class ) ) );
    writeRead(
      () -> new SplunkIndexTap( scheme, service, indexName ),
      () -> new SplunkIndexTap( scheme, service, indexName ),
      ( count, next ) -> count + next.getInteger( 1 ),
      1, 4950
    );
    }

  @Test
  public void writeLineReadLine() throws IOException
    {
    SplunkRawLine scheme = new SplunkRawLine( new Fields( "num", long.class ).append( new Fields( "line", String.class ) ), new Fields( "time", long.class ).append( new Fields( "value", int.class ) ) );
    writeRead(
      () -> new SplunkIndexTap( scheme, service, indexName ),
      () -> new SplunkIndexTap( scheme, service, indexName ),
      ( count, next ) -> count + next.getInteger( 0 ),
      1, 4950
    );
    }

  @Test
  public void writeDelimitedSearchDelimited() throws IOException
    {
    SplunkRawDelimited scheme = new SplunkRawDelimited( new Fields( "time", long.class ).append( new Fields( "value", int.class ) ) );
    writeRead(
      () -> new SplunkIndexTap( scheme, service, indexName ),
      () -> new SplunkSearchTap( scheme, service, String.format( "index=%s *", indexName ) ),
      ( count, next ) -> count + next.getInteger( 1 ),
      1, 4950
    );
    }

  protected void writeRead( Supplier<Tap> sink, Supplier<Tap> source, BiFunction<Integer, TupleEntry, Integer> aggregate, int iterations, int test ) throws IOException
    {
    Assert.assertTrue( getResultCountOfIndex( service, indexName ) == 0 );
    Assert.assertTrue( getTotalEventCount( index ) == 0 );

    int totalItems = repeat( sink, iterations );

    assertEventuallyTrue( () -> getResultCountOfIndex( service, indexName ) == totalItems );
    assertEventuallyTrue( () -> getTotalEventCount( index ) == totalItems );

    Tap input = source.get();

    TupleEntryIterator iterator = input.openForRead( FlowProcess.nullFlowProcess() );

    int count = 0;
    while( iterator.hasNext() )
      {
      TupleEntry next = iterator.next();

      System.out.println( "next = " + next );

      count = aggregate.apply( count, next );
      }

    assertEquals( test, count );
    }

  private int repeat( Supplier<Tap> sink, int iterations ) throws IOException
    {
    int totalItems = 100 / iterations;

    for( int j = 0; j < iterations; j++ )
      {
      Tap output = sink.get();

      try( TupleEntryCollector collector = output.openForWrite( FlowProcess.nullFlowProcess() ) )
        {
        for( int i = 0; i < totalItems; i++ )
          collector.add( new Tuple( System.currentTimeMillis(), i + ( j * totalItems ) ) );
        }
      }

    return totalItems * iterations;
    }
  }
