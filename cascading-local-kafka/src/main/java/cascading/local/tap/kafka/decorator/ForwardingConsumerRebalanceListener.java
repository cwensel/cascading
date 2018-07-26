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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/**
 *
 */
public class ForwardingConsumerRebalanceListener implements ConsumerRebalanceListener
  {
  ConsumerRebalanceListener listener = new NoOpConsumerRebalanceListener();

  public ForwardingConsumerRebalanceListener()
    {
    }

  public ForwardingConsumerRebalanceListener( ConsumerRebalanceListener listener )
    {
    if( listener != null )
      this.listener = listener;
    }

  @Override
  public void onPartitionsRevoked( Collection<TopicPartition> collection )
    {
    listener.onPartitionsRevoked( collection );
    }

  @Override
  public void onPartitionsAssigned( Collection<TopicPartition> collection )
    {
    listener.onPartitionsAssigned( collection );
    }
  }
