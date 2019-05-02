/*
 * Copyright (c) 2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.local.tap.kafka;

import java.io.IOException;
import java.util.function.Predicate;

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

/**
 *
 */
public class KafkaTapIntegrationTest extends CascadingTestCase
  {
  @ClassRule
  public static KafkaContainer kafka = new KafkaContainer( "5.1.0" );

  @Test
  public void writeRead() throws Exception
    {
    handle( new TextKafkaScheme(), tupleEntry -> tupleEntry.getObject( 2 ) instanceof String, "my-test-default" );
    }

  @Test
  public void writeReadTyped() throws Exception
    {
    Fields sourceFields = new Fields( "topic", String.class ).append( new Fields( "offset", Long.class ) )
      .append( new Fields( "key", Integer.class ).append( new Fields( "value", Integer.class ) )
        .append( new Fields( "timestamp", Long.class ) ).append( new Fields( "tsType", String.class ) ) );

    handle( new TextKafkaScheme( sourceFields ), tupleEntry -> tupleEntry.getObject( 2 ) instanceof Integer, "my-test-typed" );
    }

  private void handle( TextKafkaScheme text, Predicate<TupleEntry> predicate, String topic ) throws IOException
    {
    String hostname = kafka.getBootstrapServers();

    KafkaTap<String, String> tap = new KafkaTap<>( text, hostname, "test-client", topic + "-topic" );

    try( TupleEntryCollector collector = tap.openForWrite( FlowProcess.nullFlowProcess() ) )
      {
      for( int i = 0; i < 100; i++ )
        collector.add( new Tuple( i, i ) );
      }

    {
    int count = 0;
    try( TupleEntryIterator iterator = tap.openForRead( FlowProcess.nullFlowProcess() ) )
      {
      while( iterator.hasNext() && predicate.test( iterator.next() ) )
        count++;
      }

    assertEquals( 99, count );
    }

    {
    int count = 0;
    try( TupleEntryIterator iterator = tap.openForRead( FlowProcess.nullFlowProcess() ) )
      {
      while( iterator.hasNext() && iterator.next() != null )
        count++;
      }

    assertEquals( 0, count );
    }

    tap = new KafkaTap<>( text, hostname, "test-client-2", "/" + topic + "-.*/" );

    {
    int count = 0;
    try( TupleEntryIterator iterator = tap.openForRead( FlowProcess.nullFlowProcess() ) )
      {
      while( iterator.hasNext() && iterator.next() != null )
        count++;
      }

    assertEquals( 99, count );
    }
    }
  }