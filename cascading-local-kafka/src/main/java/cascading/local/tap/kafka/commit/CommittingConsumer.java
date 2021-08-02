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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import cascading.local.tap.kafka.decorator.ForwardingConsumer;
import cascading.local.tap.kafka.decorator.ForwardingConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CommittingConsumer<K, V> extends ForwardingConsumer<K, V>
  {
  private static final Logger LOG = LoggerFactory.getLogger( CommittingConsumer.class );

  Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

  CommitListener listener = new CommitListener()
    {
    @Override
    public void onClose( Consumer consumer, Map<TopicPartition, OffsetAndMetadata> offsets )
      {
      LOG.info( "committing offsets on close" );
      }

    @Override
    public void onRevoke( Consumer consumer, Map<TopicPartition, OffsetAndMetadata> offsets )
      {
      LOG.info( "committing offsets on partition revoke" );
      }

    @Override
    public boolean onFail( Consumer consumer, RuntimeException exception, Map<TopicPartition, OffsetAndMetadata> offsets )
      {
      LOG.error( "failed committing offsets", exception );

      return true;
      }
    };

  public CommittingConsumer( Properties properties, CommitListener listener )
    {
    super( properties );
    this.listener = listener;
    }

  public CommittingConsumer( Properties properties )
    {
    super( properties );
    }

  @Override
  protected KafkaConsumer<K, V> createKafkaConsumerInstance( Properties properties )
    {
    boolean autoCommitEnabled = Boolean.parseBoolean( properties.getProperty( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG ) );

    if( autoCommitEnabled )
      LOG.info( "disabling kafka auto-commit" );

    properties.setProperty( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false" );

    return super.createKafkaConsumerInstance( properties );
    }

  @Override
  public void subscribe( Collection<String> collection )
    {
    super.subscribe( collection, new CommittingRebalanceListener<>( getConsumer(), listener, currentOffsets ) );
    }

  @Override
  public void subscribe( Collection<String> collection, ConsumerRebalanceListener consumerRebalanceListener )
    {
    super.subscribe( collection, new CommittingRebalanceListener<>( consumerRebalanceListener, getConsumer(), listener, currentOffsets ) );
    }

  @Override
  public void subscribe( Pattern pattern, ConsumerRebalanceListener consumerRebalanceListener )
    {
    super.subscribe( pattern, new CommittingRebalanceListener<>( consumerRebalanceListener, getConsumer(), listener, currentOffsets ) );
    }

  @Override
  public ConsumerRecords<K, V> poll( long l )
    {
    return new ForwardingConsumerRecords<K, V>( super.poll( l ) )
      {
      @Override
      public Iterator<ConsumerRecord<K, V>> iterator()
        {
        return new OffsetRecorderIterator<>( currentOffsets, super.iterator() );
        }
      };
    }

  @Override
  public void close()
    {
    try
      {
      listener.onClose( getConsumer(), currentOffsets );
      getConsumer().commitSync( currentOffsets );
      }
    catch( RuntimeException exception )
      {
      if( listener.onFail( getConsumer(), exception, currentOffsets ) )
        throw exception;
      }
    finally
      {
      super.close();
      }
    }

  @Override
  public void close( long l, TimeUnit timeUnit )
    {
    try
      {
      listener.onClose( getConsumer(), currentOffsets );
      getConsumer().commitSync( currentOffsets );
      }
    catch( RuntimeException exception )
      {
      if( listener.onFail( getConsumer(), exception, currentOffsets ) )
        throw exception;
      }
    finally
      {
      super.close( l, timeUnit );
      }
    }
  }
