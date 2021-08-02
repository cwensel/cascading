/*
 * Copyright (c) 2017-2020 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

import java.util.Properties;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import org.apache.kafka.clients.producer.Producer;

/**
 * Class KafkaScheme is the base class used for all Schemes that are to be used with the {@link KafkaTap}.
 */
public abstract class KafkaScheme<K, V, SourceContext, SinkContext> extends Scheme<Properties, KafkaConsumerRecordIterator<K, V>, Producer<K, V>, SourceContext, SinkContext>
  {
  public KafkaScheme( Fields sourceFields )
    {
    super( sourceFields );
    }

  public KafkaScheme( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );
    }
  }
