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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import cascading.flow.FlowProcess;
import cascading.local.tap.kafka.decorator.ForwardingConsumer;
import cascading.property.PropertyUtil;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;
import cascading.util.CloseableIterator;
import cascading.util.Util;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class KafkaTap is a Cascading local mode Tap providing read and write access to data stored in Apache Kafka topics.
 * <p>
 * This Tap is not intended to be used with any of the other Cascading planners unless they specify they are local mode
 * compatible.
 * <p>
 * The Kafka producer and consumer interfaces are not Input/OutputStream based, subsequently the
 * KafkaTap must be used with a {@link KafkaScheme} sub-class which encapsulates the mechanics of reading/writing
 * from/to a Kafka topic.
 * <p>
 * Further Kafka tends to treat a topic as data vs a location, causing further coupling between the KafkaTap and
 * KafkaScheme. This may be Kafka's actual intent, so this class may need to be revised, or an alternate implementation
 * implemented.
 * <p>
 * Subsequently, the KafkaTap can be instantiated with a {@link URI} instance of the format:
 * {@code kafka://[hostname]<:port>/[topic]<,topic>}.
 * <p>
 * Where hostname and at least one topic is required.
 * <p>
 * If the first topic is wrapped by slash ({@code /}), e.g. {@code /some-.*-topic/}, the string within the slashes
 * will be considered a regular-expression. Any subsequent topics will be ignored.
 * <p>
 * Note on read, the KafkaTap will continue to retrieve data until the {@code pollTimeout} is reached, where the
 * default is 10 seconds.
 * <p>
 * Use the {@code defaultProperties} argument to set Kafka Consumer/Producer specific properties.
 * <p>
 * By default, {@link #CONSUME_AUTO_COMMIT_EARLIEST} with values:
 * <ul>
 * <li>{@code ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG} is {@code true }</li>
 * <li>{@code ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG} is {@code 1000 }</li>
 * <li>{@code ConsumerConfig.AUTO_OFFSET_RESET_CONFIG} is {@code "earliest" }</li>
 * </ul>
 * <p>
 * and {@link #PRODUCE_ACK_ALL_NO_RETRY} with values:
 * <ul>
 * <li>{@code ProducerConfig.ACKS_CONFIG} is {@code "all }</li>
 * <li>{@code ProducerConfig.RETRIES_CONFIG} is {@code 0 }</li>
 * </ul>
 *
 * @see #CONSUME_AUTO_COMMIT_EARLIEST
 * @see #CONSUME_AUTO_COMMIT_LATEST
 * @see #PRODUCE_ACK_ALL_NO_RETRY
 * @see PropertyUtil#merge(Properties...) for conveniently merging Property instances
 */
public class KafkaTap<K, V> extends Tap<Properties, Iterator<ConsumerRecord<K, V>>, Producer<K, V>>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( KafkaTap.class );

  /** Field CONSUME_AUTO_COMMIT_LATEST */
  public static final Properties CONSUME_AUTO_COMMIT_LATEST = new Properties()
    {
    {
    setProperty( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true" );
    setProperty( ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000" );
    setProperty( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest" );
    }
    };

  /** Field CONSUME_AUTO_COMMIT_EARLIEST */
  public static final Properties CONSUME_AUTO_COMMIT_EARLIEST = new Properties()
    {
    {
    setProperty( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true" );
    setProperty( ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000" );
    setProperty( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" );
    }
    };

  /** Field PRODUCE_ACK_ALL_NO_RETRY */
  public static final Properties PRODUCE_ACK_ALL_NO_RETRY = new Properties()
    {
    {
    setProperty( ProducerConfig.ACKS_CONFIG, "all" );
    setProperty( ProducerConfig.RETRIES_CONFIG, "0" );
    }
    };

  /** Field DEFAULT_POLL_TIMEOUT */
  public static final long DEFAULT_POLL_TIMEOUT = 10_000L;
  /** Field DEFAULT_REPLICATION_FACTOR */
  public static final short DEFAULT_REPLICATION_FACTOR = 1;
  /** Field DEFAULT_NUM_PARTITIONS */
  public static final int DEFAULT_NUM_PARTITIONS = 1;

  /** Field defaultProperties */
  Properties defaultProperties = PropertyUtil.merge( CONSUME_AUTO_COMMIT_EARLIEST, PRODUCE_ACK_ALL_NO_RETRY );
  /** Field hostname */
  String hostname;
  /** Field topics */
  String[] topics;
  /** Field topicIsPattern */
  boolean isTopicPattern = false;
  /** Field numPartitions */
  int numPartitions = DEFAULT_NUM_PARTITIONS;
  /** Field replicationFactor */
  short replicationFactor = DEFAULT_REPLICATION_FACTOR;
  /** Field clientID */
  String clientID = null;
  /** Field groupID */
  String groupID = Tap.id( this );
  /** Field pollTimeout */
  long pollTimeout = DEFAULT_POLL_TIMEOUT;

  /**
   * Method makeURI creates a kafka URI for use with the KafkaTap.
   *
   * @param hostname hostname and optionally port information to connect too
   * @param topics   one more topics to connect too
   * @return a URI instance
   */
  public static URI makeURI( String hostname, String... topics )
    {
    if( hostname == null )
      throw new IllegalArgumentException( "hostname may not be null" );

    Arrays.sort( topics );

    try
      {
      return new URI( "kafka", hostname, "/" + Util.join( ",", topics ), null, null );
      }
    catch( URISyntaxException exception )
      {
      throw new IllegalArgumentException( exception.getMessage(), exception );
      }
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier )
    {
    this( defaultProperties, scheme, identifier, DEFAULT_POLL_TIMEOUT, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme      of KafkaScheme
   * @param identifier  of URI
   * @param pollTimeout of long
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, long pollTimeout )
    {
    this( scheme, identifier, pollTimeout, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param numPartitions     of int
   * @param replicationFactor of short
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, int numPartitions, short replicationFactor )
    {
    this( scheme, identifier, DEFAULT_POLL_TIMEOUT, numPartitions, replicationFactor );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param pollTimeout       of int
   * @param numPartitions     of int
   * @param replicationFactor of short
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, long pollTimeout, int numPartitions, short replicationFactor )
    {
    this( null, scheme, identifier, pollTimeout, numPartitions, replicationFactor );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme     of KafkaScheme
   * @param identifier of URI
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier )
    {
    this( scheme, identifier, DEFAULT_POLL_TIMEOUT, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param pollTimeout       of int
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, long pollTimeout )
    {
    this( defaultProperties, scheme, identifier, pollTimeout, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param numPartitions     of int
   * @param replicationFactor of short
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, int numPartitions, short replicationFactor )
    {
    this( defaultProperties, scheme, identifier, DEFAULT_POLL_TIMEOUT, numPartitions, replicationFactor );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param pollTimeout       of int
   * @param numPartitions     of int
   * @param replicationFactor of short
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, long pollTimeout, int numPartitions, short replicationFactor )
    {
    this( defaultProperties, scheme, identifier, null, pollTimeout, numPartitions, replicationFactor );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param clientID          of String
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID )
    {
    this( defaultProperties, scheme, identifier, clientID, DEFAULT_POLL_TIMEOUT, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param clientID          of String
   * @param groupID           of String
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, String groupID )
    {
    this( defaultProperties, scheme, identifier, clientID, groupID, DEFAULT_POLL_TIMEOUT, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme      of KafkaScheme
   * @param identifier  of URI
   * @param clientID    of String
   * @param pollTimeout of long
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, long pollTimeout )
    {
    this( scheme, identifier, clientID, pollTimeout, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param clientID          of String
   * @param numPartitions     of int
   * @param replicationFactor of short
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, int numPartitions, short replicationFactor )
    {
    this( scheme, identifier, clientID, DEFAULT_POLL_TIMEOUT, numPartitions, replicationFactor );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param clientID          of String
   * @param pollTimeout       of int
   * @param numPartitions     of int
   * @param replicationFactor of short
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, long pollTimeout, int numPartitions, short replicationFactor )
    {
    this( null, scheme, identifier, clientID, pollTimeout, numPartitions, replicationFactor );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme     of KafkaScheme
   * @param identifier of URI
   * @param clientID   of String
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID )
    {
    this( scheme, identifier, clientID, DEFAULT_POLL_TIMEOUT, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme     of KafkaScheme
   * @param identifier of URI
   * @param clientID   of String
   * @param groupID    of String
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, String groupID )
    {
    this( null, scheme, identifier, clientID, groupID, DEFAULT_POLL_TIMEOUT, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param clientID          of String
   * @param pollTimeout       of int
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, long pollTimeout )
    {
    this( defaultProperties, scheme, identifier, clientID, pollTimeout, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param clientID          of String
   * @param numPartitions     of int
   * @param replicationFactor of short
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, int numPartitions, short replicationFactor )
    {
    this( defaultProperties, scheme, identifier, clientID, DEFAULT_POLL_TIMEOUT, numPartitions, replicationFactor );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param clientID          of String
   * @param pollTimeout       of int
   * @param numPartitions     of int
   * @param replicationFactor of short
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, long pollTimeout, int numPartitions, short replicationFactor )
    {
    this( defaultProperties, scheme, identifier, clientID, null, pollTimeout, numPartitions, replicationFactor );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param identifier        of URI
   * @param clientID          of String
   * @param groupID           of String
   * @param pollTimeout       of int
   * @param numPartitions     of int
   * @param replicationFactor of short
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, URI identifier, String clientID, String groupID, long pollTimeout, int numPartitions, short replicationFactor )
    {
    super( scheme, SinkMode.UPDATE );

    if( defaultProperties != null )
      this.defaultProperties = new Properties( defaultProperties );

    if( identifier == null )
      throw new IllegalArgumentException( "identifier may not be null" );

    if( !identifier.getScheme().equalsIgnoreCase( "kafka" ) )
      throw new IllegalArgumentException( "identifier does not have kafka scheme" );

    this.hostname = identifier.getHost();

    if( identifier.getPort() != -1 )
      this.hostname += ":" + identifier.getPort();

    if( identifier.getQuery() == null )
      throw new IllegalArgumentException( "must have at least one topic in the query part of the URI" );

    if( clientID != null )
      this.clientID = clientID;

    if( groupID != null )
      this.groupID = groupID;

    this.pollTimeout = pollTimeout;
    this.numPartitions = numPartitions;
    this.replicationFactor = replicationFactor;

    applyTopics( identifier.getQuery().split( "," ) );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme      of KafkaScheme
   * @param hostname    of String
   * @param pollTimeout of long
   * @param topics      of String...
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, String hostname, long pollTimeout, String... topics )
    {
    this( scheme, hostname, pollTimeout, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR, topics );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme            of KafkaScheme
   * @param hostname          of String
   * @param pollTimeout       of int
   * @param numPartitions     of int
   * @param replicationFactor of short
   * @param topics            of String...
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, String hostname, long pollTimeout, int numPartitions, short replicationFactor, String... topics )
    {
    this( null, scheme, hostname, pollTimeout, numPartitions, replicationFactor, topics );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param hostname          of String
   * @param numPartitions     of int
   * @param replicationFactor of short
   * @param topics            of String...
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, int numPartitions, short replicationFactor, String... topics )
    {
    this( defaultProperties, scheme, hostname, DEFAULT_POLL_TIMEOUT, numPartitions, replicationFactor, topics );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param hostname          of String
   * @param topics            of String...
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, String... topics )
    {
    this( defaultProperties, scheme, hostname, DEFAULT_POLL_TIMEOUT, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR, topics );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param hostname          of String
   * @param pollTimeout       of int
   * @param topics            of String...
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, long pollTimeout, String... topics )
    {
    this( defaultProperties, scheme, hostname, pollTimeout, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR, topics );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param hostname          of String
   * @param pollTimeout       of int
   * @param numPartitions     of int
   * @param replicationFactor of short
   * @param topics            of String...
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, long pollTimeout, int numPartitions, short replicationFactor, String... topics )
    {
    this( defaultProperties, scheme, hostname, null, pollTimeout, numPartitions, replicationFactor, topics );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme   of KafkaScheme
   * @param hostname of String
   * @param clientID of String
   * @param topics   of String...
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, String... topics )
    {
    this( scheme, hostname, clientID, DEFAULT_POLL_TIMEOUT, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR, topics );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme      of KafkaScheme
   * @param hostname    of String
   * @param clientID    of String
   * @param pollTimeout of long
   * @param topics      of String...
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, long pollTimeout, String... topics )
    {
    this( scheme, hostname, clientID, pollTimeout, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR, topics );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param scheme            of KafkaScheme
   * @param hostname          of String
   * @param clientID          of String
   * @param pollTimeout       of int
   * @param numPartitions     of int
   * @param replicationFactor of short
   * @param topics            of String...
   */
  public KafkaTap( KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, long pollTimeout, int numPartitions, short replicationFactor, String... topics )
    {
    this( null, scheme, hostname, clientID, pollTimeout, numPartitions, replicationFactor, topics );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param hostname          of String
   * @param clientID          of String
   * @param numPartitions     of int
   * @param replicationFactor of short
   * @param topics            of String...
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, int numPartitions, short replicationFactor, String... topics )
    {
    this( defaultProperties, scheme, hostname, clientID, DEFAULT_POLL_TIMEOUT, numPartitions, replicationFactor, topics );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param hostname          of String
   * @param clientID          of String
   * @param topics            of String...
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, String... topics )
    {
    this( defaultProperties, scheme, hostname, clientID, DEFAULT_POLL_TIMEOUT, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR, topics );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param hostname          of String
   * @param clientID          of String
   * @param pollTimeout       of int
   * @param topics            of String...
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, long pollTimeout, String... topics )
    {
    this( defaultProperties, scheme, hostname, clientID, pollTimeout, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR, topics );
    }

  /**
   * Constructor KafkaTap creates a new KafkaTap instance.
   *
   * @param defaultProperties of Properties
   * @param scheme            of KafkaScheme
   * @param hostname          of String
   * @param clientID          of String
   * @param pollTimeout       of int
   * @param numPartitions     of int
   * @param replicationFactor of short
   * @param topics            of String...
   */
  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, long pollTimeout, int numPartitions, short replicationFactor, String... topics )
    {
    this( defaultProperties, scheme, hostname, clientID, null, pollTimeout, numPartitions, replicationFactor, topics );
    }

  public KafkaTap( Properties defaultProperties, KafkaScheme<K, V, ?, ?> scheme, String hostname, String clientID, String groupID, long pollTimeout, int numPartitions, short replicationFactor, String... topics )
    {
    super( scheme, SinkMode.UPDATE );

    if( defaultProperties != null )
      this.defaultProperties = new Properties( defaultProperties );

    this.hostname = hostname;

    if( clientID != null )
      this.clientID = clientID;

    if( groupID != null )
      this.groupID = groupID;

    this.pollTimeout = pollTimeout;
    this.numPartitions = numPartitions;
    this.replicationFactor = replicationFactor;

    applyTopics( topics );
    }

  protected void applyTopics( String[] topics )
    {
    if( topics[ 0 ].matches( "^/([^/]|//)*/$" ) )
      {
      this.topics = new String[]{topics[ 0 ].substring( 1, topics[ 0 ].length() - 1 )};
      this.isTopicPattern = true;
      }
    else
      {
      this.topics = new String[ topics.length ];
      System.arraycopy( topics, 0, this.topics, 0, topics.length );
      }
    }

  /**
   * Method getHostname returns the hostname of this KafkaTap object.
   *
   * @return the hostname (type String) of this KafkaTap object.
   */
  public String getHostname()
    {
    return hostname;
    }

  /**
   * Method getClientID returns the clientID of this KafkaTap object.
   *
   * @return the clientID (type String) of this KafkaTap object.
   */
  public String getClientID()
    {
    return clientID;
    }

  /**
   * Method getGroupID returns the groupID of this KafkaTap object.
   *
   * @return the groupID (type String) of this KafkaTap object.
   */
  public String getGroupID()
    {
    return groupID;
    }

  /**
   * Method getTopics returns the topics of this KafkaTap object.
   *
   * @return the topics (type String[]) of this KafkaTap object.
   */
  public String[] getTopics()
    {
    return topics;
    }

  /**
   * Method isTopicPattern returns true if the topic is a regular expression.
   *
   * @return true if the topic is a regular expression.
   */
  public boolean isTopicPattern()
    {
    return isTopicPattern;
    }

  @Override
  public String getIdentifier()
    {
    return makeURI( hostname, topics ).toString();
    }

  protected Consumer<K, V> createKafkaConsumer( Properties properties )
    {
    return new ForwardingConsumer<>( properties );
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Properties> flowProcess, Iterator<ConsumerRecord<K, V>> consumerRecord ) throws IOException
    {
    Properties props = PropertyUtil.merge( flowProcess.getConfig(), defaultProperties );

    props.setProperty( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostname );

    Set<String> keys = props.stringPropertyNames();

    if( clientID != null && !keys.contains( ConsumerConfig.CLIENT_ID_CONFIG ) )
      props.setProperty( ConsumerConfig.CLIENT_ID_CONFIG, clientID );

    // allows all calls from this instance to share a group
    if( !keys.contains( ConsumerConfig.GROUP_ID_CONFIG ) )
      props.setProperty( ConsumerConfig.GROUP_ID_CONFIG, groupID );

    sourceConfInit( flowProcess, props );

    Consumer<K, V> consumer = createKafkaConsumer( PropertyUtil.retain( props, ConsumerConfig.configNames() ) );

    preConsumerSubscribe( consumer );

    if( isTopicPattern )
      consumer.subscribe( Pattern.compile( topics[ 0 ] ), getConsumerRebalanceListener( consumer ) );
    else
      consumer.subscribe( Arrays.asList( getTopics() ), getConsumerRebalanceListener( consumer ) );

    postConsumerSubscribe( consumer );

    CloseableIterator<Iterator<ConsumerRecord<K, V>>> iterator = new CloseableIterator<Iterator<ConsumerRecord<K, V>>>()
      {
      boolean completed = false;
      ConsumerRecords<K, V> records;

      @Override
      public boolean hasNext()
        {
        if( records != null )
          return true;

        if( completed )
          return false;

        records = consumer.poll( pollTimeout );

        LOG.debug( "kafka records polled: {}", records.count() );

        if( records.isEmpty() )
          {
          completed = true;
          records = null;
          }

        return records != null;
        }

      @Override
      public Iterator<ConsumerRecord<K, V>> next()
        {
        if( !hasNext() )
          throw new NoSuchElementException( "no more elements" );

        try
          {
          return records.iterator();
          }
        finally
          {
          records = null;
          }
        }

      @Override
      public void close()
        {
        try
          {
          consumer.close();
          }
        finally
          {
          completed = true;
          }
        }
      };

    return new TupleEntrySchemeIterator<Properties, Iterator<ConsumerRecord<K, V>>>( flowProcess, this, getScheme(), iterator );
    }

  /**
   * Prepare {@link Consumer} prior to any topic subscription.
   *
   * @param consumer the current Consumer
   */
  protected void preConsumerSubscribe( Consumer<K, V> consumer )
    {

    }

  /**
   * Prepare {@link Consumer} post to any topic subscription and prior to any {@link Consumer#poll(long)} request.
   *
   * @param consumer the current Consumer
   */
  protected void postConsumerSubscribe( Consumer<K, V> consumer )
    {

    }

  /**
   * Returns a {@link NoOpConsumerRebalanceListener} instance.
   * <p>
   * Override to supply a customer listener.
   *
   * @param consumer
   * @return a NoOpConsumerRebalanceListener instance.
   */
  protected ConsumerRebalanceListener getConsumerRebalanceListener( Consumer<K, V> consumer )
    {
    return new NoOpConsumerRebalanceListener();
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Properties> flowProcess, Producer<K, V> producer ) throws IOException
    {
    Properties props = PropertyUtil.merge( flowProcess.getConfig(), defaultProperties );

    props.setProperty( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostname );

    sinkConfInit( flowProcess, props );

    producer = new KafkaProducer<>( PropertyUtil.retain( props, ProducerConfig.configNames() ) );

    return new TupleEntrySchemeCollector<Properties, Producer<?, ?>>( flowProcess, this, getScheme(), producer );
    }

  protected AdminClient createAdminClient( Properties conf )
    {
    Properties props = new Properties( conf );

    props.setProperty( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostname );

    return AdminClient.create( props );
    }

  @Override
  public boolean createResource( Properties conf )
    {
    AdminClient client = createAdminClient( conf );

    List<NewTopic> topics = new ArrayList<>( getTopics().length );

    for( String topic : getTopics() )
      topics.add( new NewTopic( topic, numPartitions, replicationFactor ) );

    CreateTopicsResult result = client.createTopics( topics );

    KafkaFuture<Void> all = result.all();

    try
      {
      all.get();
      }
    catch( InterruptedException | ExecutionException exception )
      {
      LOG.info( "unable to create topics" );

      return false;
      }

    return true;
    }

  @Override
  public boolean deleteResource( Properties conf )
    {
    AdminClient client = createAdminClient( conf );

    DeleteTopicsResult result = client.deleteTopics( Arrays.asList( getTopics() ) );

    KafkaFuture<Void> all = result.all();

    try
      {
      all.get();
      }
    catch( InterruptedException | ExecutionException exception )
      {
      LOG.info( "unable to create topics" );

      return false;
      }

    return true;
    }

  @Override
  public boolean resourceExists( Properties conf )
    {
    AdminClient client = createAdminClient( conf );

    DescribeTopicsResult result = client.describeTopics( Arrays.asList( getTopics() ) );

    KafkaFuture<Map<String, TopicDescription>> all = result.all();

    try
      {
      Map<String, TopicDescription> map = all.get();

      return map.size() == getTopics().length;
      }
    catch( InterruptedException | ExecutionException exception )
      {
      LOG.info( "unable to create topics" );

      return false;
      }
    }

  @Override
  public long getModifiedTime( Properties conf ) throws IOException
    {
    if( resourceExists( conf ) )
      return Long.MAX_VALUE;
    else
      return 0L;
    }
  }
