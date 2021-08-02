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

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * CommitListener is a listener that notifies on offset commits and any subsequent failures.
 */
public interface CommitListener
  {
  default void onClose( Consumer consumer, Map<TopicPartition, OffsetAndMetadata> offsets )
    {
    }

  default void onRevoke( Consumer consumer, Map<TopicPartition, OffsetAndMetadata> offsets )
    {
    }

  /**
   * @param exception the caught exception
   * @param offsets   current partition offsets
   * @return returns true if the captured exception should be rethrown
   */
  default boolean onFail( Consumer consumer, RuntimeException exception, Map<TopicPartition, OffsetAndMetadata> offsets )
    {
    return true;
    }
  }