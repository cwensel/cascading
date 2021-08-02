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

package cascading.local.tap.kafka.decorator;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 *
 */
public class ForwardingConsumerRecords<K, V> extends ConsumerRecords<K, V>
  {
  ConsumerRecords<K, V> consumerRecords;

  public ForwardingConsumerRecords( ConsumerRecords<K, V> consumerRecords )
    {
    super( null );
    this.consumerRecords = consumerRecords;
    }

  @Override
  public List<ConsumerRecord<K, V>> records( TopicPartition partition )
    {
    return consumerRecords.records( partition );
    }

  @Override
  public Iterable<ConsumerRecord<K, V>> records( String topic )
    {
    return consumerRecords.records( topic );
    }

  @Override
  public Set<TopicPartition> partitions()
    {
    return consumerRecords.partitions();
    }

  @Override
  public Iterator<ConsumerRecord<K, V>> iterator()
    {
    return consumerRecords.iterator();
    }

  @Override
  public int count()
    {
    return consumerRecords.count();
    }

  @Override
  public boolean isEmpty()
    {
    return consumerRecords.isEmpty();
    }

  public static <K1, V1> ConsumerRecords<K1, V1> empty()
    {
    return ConsumerRecords.empty();
    }
  }
