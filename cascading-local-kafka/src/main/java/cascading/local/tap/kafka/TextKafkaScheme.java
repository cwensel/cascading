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

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Class TextKafkaScheme is a sub-class of the {@link KafkaScheme} for use with a {@link KafkaTap} instance.
 * <p>
 * It consumes and produces text/string based keys and values.
 * <p>
 * As a source, it produces six fields: {@link #TOPIC_FIELDS} typed String, {@link #OFFSET_FIELDS} typed long,
 * {@link #KEY_FIELDS} typed String, and {@link #VALUE_FIELDS} typed String,
 * {@link #TIMESTAMP_FIELDS} typed long, {@link #TIMESTAMP_TYPE_FIELDS} typed String
 * <p>
 * If alternate source fields are given, any type information will be honored.
 * <p>
 * As a sink, the first field encountered will be used as the topic key, and the second field encountered will be
 * used as the value.
 */
public class TextKafkaScheme extends KafkaScheme<String, String, TextKafkaScheme.Context, TextKafkaScheme.Context>
  {
  /** Field TOPIC_FIELDS */
  public static final Fields TOPIC_FIELDS = new Fields( "topic", String.class );
  /** Field OFFSET_FIELDS */
  public static final Fields OFFSET_FIELDS = new Fields( "offset", long.class );
  /** Field KEY_FIELDS */
  public static final Fields KEY_FIELDS = new Fields( "key", String.class );
  /** Field VALUE_FIELDS */
  public static final Fields VALUE_FIELDS = new Fields( "value", String.class );
  /** Field TIMESTAMP_FIELDS */
  public static final Fields TIMESTAMP_FIELDS = new Fields( "timestamp", long.class );
  /** Field TIMESTAMP_TYPE_FIELDS */
  public static final Fields TIMESTAMP_TYPE_FIELDS = new Fields( "timestampType", String.class );
  /** Field DEFAULT_SOURCE_FIELDS */
  public static final Fields DEFAULT_SOURCE_FIELDS = TOPIC_FIELDS.append( OFFSET_FIELDS ).append( KEY_FIELDS ).append( VALUE_FIELDS ).append( TIMESTAMP_FIELDS ).append( TIMESTAMP_TYPE_FIELDS );

  class Context
    {
    String[] topics;

    public Context( String[] topics )
      {
      this.topics = topics;
      }
    }

  /**
   * Constructor TextKafkaScheme creates a new TextKafkaScheme instance.
   */
  public TextKafkaScheme()
    {
    super( DEFAULT_SOURCE_FIELDS );
    }

  /**
   * Constructor TextKafkaScheme creates a new TextKafkaScheme instance.
   *
   * @param sourceFields of Fields
   */
  public TextKafkaScheme( Fields sourceFields )
    {
    super( sourceFields );

    if( sourceFields.size() != 6 )
      throw new IllegalArgumentException( "wrong number of source fields, requires 6, got: " + sourceFields );
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, KafkaConsumerRecordIterator<String, String>, Producer<String, String>> tap, Properties conf )
    {
    conf.setProperty( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName() );
    conf.setProperty( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName() );
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, KafkaConsumerRecordIterator<String, String>, Producer<String, String>> tap, Properties conf )
    {
    conf.setProperty( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName() );
    conf.setProperty( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName() );
    }

  @Override
  public void sourcePrepare( FlowProcess<? extends Properties> flowProcess, SourceCall<TextKafkaScheme.Context, KafkaConsumerRecordIterator<String, String>> sourceCall ) throws IOException
    {
    sourceCall.setContext( new Context( ( (KafkaTap) sourceCall.getTap() ).getTopics() ) );
    }

  @Override
  public void sinkPrepare( FlowProcess<? extends Properties> flowProcess, SinkCall<TextKafkaScheme.Context, Producer<String, String>> sinkCall ) throws IOException
    {
    sinkCall.setContext( new Context( ( (KafkaTap) sinkCall.getTap() ).getTopics() ) );
    }

  @Override
  public boolean source( FlowProcess<? extends Properties> flowProcess, SourceCall<TextKafkaScheme.Context, KafkaConsumerRecordIterator<String, String>> sourceCall ) throws IOException
    {
    Iterator<ConsumerRecord<String, String>> input = sourceCall.getInput();

    if( !input.hasNext() )
      return false;

    ConsumerRecord<String, String> record = input.next();
    TupleEntry incomingEntry = sourceCall.getIncomingEntry();

    // honor declared type information via #setObject()
    incomingEntry.setObject( 0, record.topic() );
    incomingEntry.setObject( 1, record.offset() );
    incomingEntry.setObject( 2, record.key() );
    incomingEntry.setObject( 3, record.value() );
    incomingEntry.setObject( 4, record.timestamp() );
    incomingEntry.setObject( 5, record.timestampType() );

    return true;
    }

  @Override
  public void sink( FlowProcess<? extends Properties> flowProcess, SinkCall<TextKafkaScheme.Context, Producer<String, String>> sinkCall ) throws IOException
    {
    // consider tap only providing bytes consumer and going ot byte here
    String key = sinkCall.getOutgoingEntry().getString( 0 );
    String value = sinkCall.getOutgoingEntry().getString( 1 );

    for( String topic : sinkCall.getContext().topics )
      sinkCall.getOutput().send( new ProducerRecord<>( topic, key, value ) );
    }
  }
