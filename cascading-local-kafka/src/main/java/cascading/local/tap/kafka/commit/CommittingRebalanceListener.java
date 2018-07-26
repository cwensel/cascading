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
import java.util.Map;

import cascading.local.tap.kafka.decorator.ForwardingConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 *
 */
public class CommittingRebalanceListener<K, V> extends ForwardingConsumerRebalanceListener
  {
  Consumer<K, V> consumer;
  CommitListener commitListener;
  Map<TopicPartition, OffsetAndMetadata> currentOffsets;

  public CommittingRebalanceListener( Consumer<K, V> consumer, CommitListener commitListener, Map<TopicPartition, OffsetAndMetadata> currentOffsets )
    {
    this.consumer = consumer;
    this.commitListener = commitListener;
    this.currentOffsets = currentOffsets;
    }

  public CommittingRebalanceListener( ConsumerRebalanceListener listener, Consumer<K, V> consumer, CommitListener commitListener, Map<TopicPartition, OffsetAndMetadata> currentOffsets )
    {
    super( listener );
    this.consumer = consumer;
    this.commitListener = commitListener;
    this.currentOffsets = currentOffsets;
    }

  @Override
  public void onPartitionsRevoked( Collection<TopicPartition> collection )
    {
    super.onPartitionsRevoked( collection );

    commitListener.onRevoke( consumer, currentOffsets );

    try
      {
      consumer.commitSync( currentOffsets );
      }
    catch( RuntimeException exception )
      {
      if( commitListener.onFail( consumer, exception, currentOffsets ) )
        throw exception;
      }
    }
  }
