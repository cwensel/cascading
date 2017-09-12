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
 * As a source, it produces four fields: {@link #TOPIC_FIELDS}, {@link #OFFSET_FIELDS}, {@link #KEY_FIELDS}, and
 * {@link #VALUE_FIELDS}.
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
  /** Field DEFAULT_SOURCE_FIELDS */
  public static final Fields DEFAULT_SOURCE_FIELDS = TOPIC_FIELDS.append( OFFSET_FIELDS ).append( KEY_FIELDS ).append( VALUE_FIELDS );

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

    if( sourceFields.size() != 4 )
      throw new IllegalArgumentException( "wrong number of source fields, requires 4, got: " + sourceFields );
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, Iterator<ConsumerRecord<String, String>>, Producer<String, String>> tap, Properties conf )
    {
    conf.setProperty( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName() );
    conf.setProperty( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName() );
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, Iterator<ConsumerRecord<String, String>>, Producer<String, String>> tap, Properties conf )
    {
    conf.setProperty( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName() );
    conf.setProperty( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName() );
    }

  @Override
  public void sourcePrepare( FlowProcess<? extends Properties> flowProcess, SourceCall<TextKafkaScheme.Context, Iterator<ConsumerRecord<String, String>>> sourceCall ) throws IOException
    {
    sourceCall.setContext( new Context( ( (KafkaTap) sourceCall.getTap() ).getTopics() ) );
    }

  @Override
  public void sinkPrepare( FlowProcess<? extends Properties> flowProcess, SinkCall<TextKafkaScheme.Context, Producer<String, String>> sinkCall ) throws IOException
    {
    sinkCall.setContext( new Context( ( (KafkaTap) sinkCall.getTap() ).getTopics() ) );
    }

  @Override
  public boolean source( FlowProcess<? extends Properties> flowProcess, SourceCall<TextKafkaScheme.Context, Iterator<ConsumerRecord<String, String>>> sourceCall ) throws IOException
    {
    Iterator<ConsumerRecord<String, String>> input = sourceCall.getInput();

    if( input.hasNext() )
      {
      ConsumerRecord<String, String> record = input.next();
      TupleEntry incomingEntry = sourceCall.getIncomingEntry();

      incomingEntry.setString( 0, record.topic() );
      incomingEntry.setLong( 1, record.offset() );
      incomingEntry.setString( 2, record.key() );
      incomingEntry.setString( 3, record.value() );
      }

    return input.hasNext();
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