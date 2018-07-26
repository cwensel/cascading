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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 *
 */
public class ForwardingConsumer<K, V> implements Consumer<K, V>
  {
  Consumer<K, V> consumer;

  public ForwardingConsumer( Properties properties )
    {
    this.consumer = createKafkaConsumerInstance( properties );
    }

  protected KafkaConsumer<K, V> createKafkaConsumerInstance( Properties properties )
    {
    return new KafkaConsumer<>( properties );
    }

  public Consumer<K, V> getConsumer()
    {
    return consumer;
    }

  @Override
  public Set<TopicPartition> assignment()
    {
    return consumer.assignment();
    }

  @Override
  public Set<String> subscription()
    {
    return consumer.subscription();
    }

  @Override
  public void subscribe( Collection<String> collection )
    {
    consumer.subscribe( collection );
    }

  @Override
  public void subscribe( Collection<String> collection, ConsumerRebalanceListener consumerRebalanceListener )
    {
    consumer.subscribe( collection, consumerRebalanceListener );
    }

  @Override
  public void assign( Collection<TopicPartition> collection )
    {
    consumer.assign( collection );
    }

  @Override
  public void subscribe( Pattern pattern, ConsumerRebalanceListener consumerRebalanceListener )
    {
    consumer.subscribe( pattern, consumerRebalanceListener );
    }

  @Override
  public void unsubscribe()
    {
    consumer.unsubscribe();
    }

  @Override
  public ConsumerRecords<K, V> poll( long l )
    {
    return consumer.poll( l );
    }

  @Override
  public void commitSync()
    {
    consumer.commitSync();
    }

  @Override
  public void commitSync( Map<TopicPartition, OffsetAndMetadata> map )
    {
    consumer.commitSync( map );
    }

  @Override
  public void commitAsync()
    {
    consumer.commitAsync();
    }

  @Override
  public void commitAsync( OffsetCommitCallback offsetCommitCallback )
    {
    consumer.commitAsync( offsetCommitCallback );
    }

  @Override
  public void commitAsync( Map<TopicPartition, OffsetAndMetadata> map, OffsetCommitCallback offsetCommitCallback )
    {
    consumer.commitAsync( map, offsetCommitCallback );
    }

  @Override
  public void seek( TopicPartition topicPartition, long l )
    {
    consumer.seek( topicPartition, l );
    }

  @Override
  public void seekToBeginning( Collection<TopicPartition> collection )
    {
    consumer.seekToBeginning( collection );
    }

  @Override
  public void seekToEnd( Collection<TopicPartition> collection )
    {
    consumer.seekToEnd( collection );
    }

  @Override
  public long position( TopicPartition topicPartition )
    {
    return consumer.position( topicPartition );
    }

  @Override
  public OffsetAndMetadata committed( TopicPartition topicPartition )
    {
    return consumer.committed( topicPartition );
    }

  @Override
  public Map<MetricName, ? extends Metric> metrics()
    {
    return consumer.metrics();
    }

  @Override
  public List<PartitionInfo> partitionsFor( String string )
    {
    return consumer.partitionsFor( string );
    }

  @Override
  public Map<String, List<PartitionInfo>> listTopics()
    {
    return consumer.listTopics();
    }

  @Override
  public Set<TopicPartition> paused()
    {
    return consumer.paused();
    }

  @Override
  public void pause( Collection<TopicPartition> collection )
    {
    consumer.pause( collection );
    }

  @Override
  public void resume( Collection<TopicPartition> collection )
    {
    consumer.resume( collection );
    }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes( Map<TopicPartition, Long> map )
    {
    return consumer.offsetsForTimes( map );
    }

  @Override
  public Map<TopicPartition, Long> beginningOffsets( Collection<TopicPartition> collection )
    {
    return consumer.beginningOffsets( collection );
    }

  @Override
  public Map<TopicPartition, Long> endOffsets( Collection<TopicPartition> collection )
    {
    return consumer.endOffsets( collection );
    }

  @Override
  public void close()
    {
    consumer.close();
    }

  @Override
  public void close( long l, TimeUnit timeUnit )
    {
    consumer.close( l, timeUnit );
    }

  @Override
  public void wakeup()
    {
    consumer.wakeup();
    }
  }
