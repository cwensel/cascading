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

package cascading.local.tap.kafka;

import java.net.URI;
import java.util.Properties;

import cascading.local.tap.kafka.commit.CommittingConsumer;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * Class KafkaCommittingTap commits consumer record offsets on completion of successful or failed flows, and revoked
 * partitions from a rebalance.
 * <p>
 * if the flow failed, the failed record will be the next record to read on that given partition.
 *
 * @param <K> the type parameter
 * @param <V> the type parameter
 */
public class KafkaCommittingTap<K, V> extends KafkaTap<K, V>
  {
  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param identifier        the identifier
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier )
    {
    super( defaultProperties, scheme, identifier );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme      the scheme
   * @param identifier  the identifier
   * @param pollTimeout the poll timeout
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, long pollTimeout )
    {
    super( scheme, identifier, pollTimeout );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, int numPartitions, short replicationFactor )
    {
    super( scheme, identifier, numPartitions, replicationFactor );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param pollTimeout       the poll timeout
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, long pollTimeout, int numPartitions, short replicationFactor )
    {
    super( scheme, identifier, pollTimeout, numPartitions, replicationFactor );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme     the scheme
   * @param identifier the identifier
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier )
    {
    super( scheme, identifier );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param pollTimeout       the poll timeout
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, long pollTimeout )
    {
    super( defaultProperties, scheme, identifier, pollTimeout );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, int numPartitions, short replicationFactor )
    {
    super( defaultProperties, scheme, identifier, numPartitions, replicationFactor );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param pollTimeout       the poll timeout
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, long pollTimeout, int numPartitions, short replicationFactor )
    {
    super( defaultProperties, scheme, identifier, pollTimeout, numPartitions, replicationFactor );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param clientID          the client id
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID )
    {
    super( defaultProperties, scheme, identifier, clientID );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param clientID          the client id
   * @param groupID           the group id
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, String groupID )
    {
    super( defaultProperties, scheme, identifier, clientID, groupID );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme      the scheme
   * @param identifier  the identifier
   * @param clientID    the client id
   * @param pollTimeout the poll timeout
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, long pollTimeout )
    {
    super( scheme, identifier, clientID, pollTimeout );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param clientID          the client id
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, int numPartitions, short replicationFactor )
    {
    super( scheme, identifier, clientID, numPartitions, replicationFactor );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param clientID          the client id
   * @param pollTimeout       the poll timeout
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, long pollTimeout, int numPartitions, short replicationFactor )
    {
    super( scheme, identifier, clientID, pollTimeout, numPartitions, replicationFactor );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme     the scheme
   * @param identifier the identifier
   * @param clientID   the client id
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID )
    {
    super( scheme, identifier, clientID );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme     the scheme
   * @param identifier the identifier
   * @param clientID   the client id
   * @param groupID    the group id
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, String groupID )
    {
    super( scheme, identifier, clientID, groupID );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param clientID          the client id
   * @param pollTimeout       the poll timeout
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, long pollTimeout )
    {
    super( defaultProperties, scheme, identifier, clientID, pollTimeout );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param clientID          the client id
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, int numPartitions, short replicationFactor )
    {
    super( defaultProperties, scheme, identifier, clientID, numPartitions, replicationFactor );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param clientID          the client id
   * @param pollTimeout       the poll timeout
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, long pollTimeout, int numPartitions, short replicationFactor )
    {
    super( defaultProperties, scheme, identifier, clientID, pollTimeout, numPartitions, replicationFactor );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param identifier        the identifier
   * @param clientID          the client id
   * @param groupID           the group id
   * @param pollTimeout       the poll timeout
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, String groupID, long pollTimeout, int numPartitions, short replicationFactor )
    {
    super( defaultProperties, scheme, identifier, clientID, groupID, pollTimeout, numPartitions, replicationFactor );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme      the scheme
   * @param hostname    the hostname
   * @param pollTimeout the poll timeout
   * @param topics      the topics
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, String hostname, long pollTimeout, String... topics )
    {
    super( scheme, hostname, pollTimeout, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme            the scheme
   * @param hostname          the hostname
   * @param pollTimeout       the poll timeout
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   * @param topics            the topics
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, String hostname, long pollTimeout, int numPartitions, short replicationFactor, String... topics )
    {
    super( scheme, hostname, pollTimeout, numPartitions, replicationFactor, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param hostname          the hostname
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   * @param topics            the topics
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, int numPartitions, short replicationFactor, String... topics )
    {
    super( defaultProperties, scheme, hostname, numPartitions, replicationFactor, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param hostname          the hostname
   * @param topics            the topics
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, String... topics )
    {
    super( defaultProperties, scheme, hostname, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param hostname          the hostname
   * @param pollTimeout       the poll timeout
   * @param topics            the topics
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, long pollTimeout, String... topics )
    {
    super( defaultProperties, scheme, hostname, pollTimeout, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param hostname          the hostname
   * @param pollTimeout       the poll timeout
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   * @param topics            the topics
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, long pollTimeout, int numPartitions, short replicationFactor, String... topics )
    {
    super( defaultProperties, scheme, hostname, pollTimeout, numPartitions, replicationFactor, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme   the scheme
   * @param hostname the hostname
   * @param clientID the client id
   * @param topics   the topics
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, String... topics )
    {
    super( scheme, hostname, clientID, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme      the scheme
   * @param hostname    the hostname
   * @param clientID    the client id
   * @param pollTimeout the poll timeout
   * @param topics      the topics
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, long pollTimeout, String... topics )
    {
    super( scheme, hostname, clientID, pollTimeout, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param scheme            the scheme
   * @param hostname          the hostname
   * @param clientID          the client id
   * @param pollTimeout       the poll timeout
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   * @param topics            the topics
   */
  public KafkaCommittingTap( KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, long pollTimeout, int numPartitions, short replicationFactor, String... topics )
    {
    super( scheme, hostname, clientID, pollTimeout, numPartitions, replicationFactor, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param hostname          the hostname
   * @param clientID          the client id
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   * @param topics            the topics
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, int numPartitions, short replicationFactor, String... topics )
    {
    super( defaultProperties, scheme, hostname, clientID, numPartitions, replicationFactor, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param hostname          the hostname
   * @param clientID          the client id
   * @param topics            the topics
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, String... topics )
    {
    super( defaultProperties, scheme, hostname, clientID, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param hostname          the hostname
   * @param clientID          the client id
   * @param pollTimeout       the poll timeout
   * @param topics            the topics
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, long pollTimeout, String... topics )
    {
    super( defaultProperties, scheme, hostname, clientID, pollTimeout, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param hostname          the hostname
   * @param clientID          the client id
   * @param pollTimeout       the poll timeout
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   * @param topics            the topics
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, long pollTimeout, int numPartitions, short replicationFactor, String... topics )
    {
    super( defaultProperties, scheme, hostname, clientID, pollTimeout, numPartitions, replicationFactor, topics );
    }

  /**
   * Instantiates a new Committing kafka tap.
   *
   * @param defaultProperties the default properties
   * @param scheme            the scheme
   * @param hostname          the hostname
   * @param clientID          the client id
   * @param groupID           the group id
   * @param pollTimeout       the poll timeout
   * @param numPartitions     the num partitions
   * @param replicationFactor the replication factor
   * @param topics            the topics
   */
  public KafkaCommittingTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, String groupID, long pollTimeout, int numPartitions, short replicationFactor, String... topics )
    {
    super( defaultProperties, scheme, hostname, clientID, groupID, pollTimeout, numPartitions, replicationFactor, topics );
    }

  @Override
  protected Consumer<K, V> createKafkaConsumer( Properties properties )
    {
    return new CommittingConsumer<>( properties );
    }
  }
