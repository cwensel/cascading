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

package cascading.local.tap.kafka.commit;

import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 *
 */
public class OffsetRecorderIterator<K, V> implements Iterator<ConsumerRecord<K, V>>
  {
  private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;
  private final Iterator<ConsumerRecord<K, V>> iterator;

  private String topic;
  private int partition;
  private long offset;

  public OffsetRecorderIterator( Map<TopicPartition, OffsetAndMetadata> currentOffsets, Iterator<ConsumerRecord<K, V>> iterator )
    {
    this.currentOffsets = currentOffsets;
    this.iterator = iterator;
    }

  @Override
  public boolean hasNext()
    {
    boolean hasNext = iterator.hasNext();

    if( !hasNext )
      addLastOffset();

    return hasNext;
    }

  @Override
  public ConsumerRecord<K, V> next()
    {
    addLastOffset(); // if the next value causes a failure, we will start with it on the next attempt

    ConsumerRecord<K, V> next = iterator.next();

    topic = next.topic();
    partition = next.partition();
    offset = next.offset();

    return next;
    }

  private void addLastOffset()
    {
    if( topic == null )
      return;

    currentOffsets.put( new TopicPartition( topic, partition ), new OffsetAndMetadata( offset + 1 ) );
    }
  }
