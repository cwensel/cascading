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

package cascading.aws.s3.logs;

import java.io.IOException;
import java.net.URI;
import java.util.TimeZone;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.local.tap.aws.s3.S3FileCheckpointer;
import cascading.local.tap.aws.s3.S3Tap;
import cascading.local.tap.kafka.KafkaTap;
import cascading.local.tap.kafka.TextKafkaScheme;
import cascading.operation.Debug;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateFormatter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.DirTap;
import cascading.tap.local.PartitionTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.type.DateType;

import static cascading.flow.FlowDef.flowDef;
import static cascading.local.tap.kafka.TextKafkaScheme.OFFSET_FIELDS;
import static cascading.local.tap.kafka.TextKafkaScheme.TOPIC_FIELDS;

/**
 * A trivial application that can read S3 logs from a S3 bucket, place them into a Kafka topic,
 * the the logs from the topic, parse them, and write them to directories partitioned on log values.
 */
public class Main
  {
  public static final String DD_MMM_YYYY = "dd-MMM-yyyy";
  public static final TimeZone UTC = TimeZone.getTimeZone( "UTC" );
  public static final DateType DMY = new DateType( DD_MMM_YYYY, UTC );
  public static final Fields KEY = new Fields( "date", DMY );
  public static final Fields LINE = new Fields( "line", String.class );
  public static final Fields KEY_LINE = KEY.append( LINE );

  public static void main( String[] args ) throws IOException
    {
    if( args.length < 3 )
      return;

    System.out.println( "source s3 uri = " + args[ 0 ] );
    System.out.println( "kafka host = " + args[ 1 ] );
    System.out.println( "sink file path = " + args[ 2 ] );

    if( args.length == 4 )
      System.out.println( "checkpoint file path = " + args[ 3 ] );

    // read from an S3 bucket
    // optionally restart where a previous run left off
    S3FileCheckpointer checkpointer = args.length == 4 ? new S3FileCheckpointer() : new S3FileCheckpointer( args[ 3 ] );
    Tap inputTap = new S3Tap( new TextLine(), checkpointer, URI.create( args[ 0 ] ) );

    // write and read from a Kafka queue
    Tap queueTap = new KafkaTap<>( new TextKafkaScheme( TOPIC_FIELDS.append( OFFSET_FIELDS ).append( KEY_LINE ) ), args[ 1 ], "parsers", "logs" );

    // write to disk, using log data to create the directory structure
    // if file exists, append to it -- we aren't duplicating s3 reads so this is safe
    DelimitedPartition partitioner = new DelimitedPartition( KEY.append( S3Logs.OPERATION ), "/", "logs.csv" );
    Tap outputTap = new PartitionTap(
      new DirTap( new TextDelimited( true, ",", "\"" ), args[ 2 ], SinkMode.UPDATE ), partitioner
    );

    Pipe ingress = new Pipe( "head" );

    // extract the log timestamp and reduce to day/month/year for use as the queue key
    ingress = new Each( ingress, new Fields( "line" ), new RegexParser( S3Logs.TIME, S3Logs.REGEX, 3 ), new Fields( "time", "line" ) );
    ingress = new Each( ingress, S3Logs.TIME, new DateFormatter( KEY, DD_MMM_YYYY, UTC ), KEY_LINE );

    // watch the progress on the console
    ingress = new Each( ingress, new Debug( true ) );

    Flow ingressFlow = new LocalFlowConnector().connect( flowDef()
      .setName( "ingress" )
      .addSource( ingress, inputTap )
      .addSink( ingress, queueTap )
      .addTail( ingress )
    );

    // start reading from S3 and writing to a Kafka queue
    ingressFlow.start();

    Pipe egress = new Pipe( "head" );

    // parse the full log into its fields and primitive values -- S3Logs.FIELDS declard field names and field types
    egress = new Each( egress, new Fields( "line" ), new RegexParser( S3Logs.FIELDS, S3Logs.REGEX ), KEY.append( S3Logs.FIELDS ) );

    // watch the progress on the console
    egress = new Each( egress, new Debug( true ) );

    Flow egressFlow = new LocalFlowConnector().connect( flowDef()
      .setName( "egress" )
      .addSource( egress, queueTap )
      .addSink( egress, outputTap )
      .addTail( egress )
    );

    // start reading from the Kafka queue and writing to the directory as ./[dd-MMM-yyyy]/[S3 operation]/logs.csv
    egressFlow.start();

    egressFlow.complete();
    System.out.println( "completed egress" );
    ingressFlow.complete();
    System.out.println( "completed ingress" );
    }
  }
