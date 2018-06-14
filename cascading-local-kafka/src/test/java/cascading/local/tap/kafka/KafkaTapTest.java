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

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import info.batey.kafka.unit.KafkaUnitRule;
import org.junit.ClassRule;
import org.junit.Test;

/**
 *
 */
public class KafkaTapTest extends CascadingTestCase
  {
  @ClassRule
  public static KafkaUnitRule kafkaUnitRule = new KafkaUnitRule( 6666, 6677, 32 );

  @Test
  public void writeRead() throws Exception
    {
    String hostname = kafkaUnitRule.getKafkaUnit().getKafkaConnect();

    TextKafkaScheme text = new TextKafkaScheme();
    KafkaTap<String, String> tap = new KafkaTap<>( text, hostname, "test-client", "my-test-topic" );

    try( TupleEntryCollector collector = tap.openForWrite( FlowProcess.nullFlowProcess() ) )
      {
      for( int i = 0; i < 100; i++ )
        collector.add( new Tuple( i, i ) );
      }

    {
    int count = 0;
    try( TupleEntryIterator iterator = tap.openForRead( FlowProcess.nullFlowProcess() ) )
      {
      while( iterator.hasNext() && iterator.next() != null )
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

    tap = new KafkaTap<>( text, hostname, "test-client-2", "/my-test-.*/" );

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