/*
 * Copyright (c) 2007-2023 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.local.tap.aws.s3;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.local.util.S3Rule;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.PartitionTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleStream;
import org.junit.Rule;
import org.junit.Test;

/**
 *
 */
public class S3TapTest extends CascadingTestCase
  {
  private final String bucketName = "bucket";

  @Rule
  public S3Rule s3Rule = new S3Rule( this::getOutputPath );

  @Test
  public void writeRead() throws Exception
    {
    String key = "write-read/";
    TextDelimited textDelimited = new TextDelimited( new Fields( "value", int.class ) );

    int totalItems = 100;

    for( int i = 0; i < totalItems; i++ )
      {
      S3Tap output = new S3Tap( textDelimited, s3Rule.get3Client(), bucketName, key + i );

      try( TupleEntryCollector collector = output.openForWrite( FlowProcess.nullFlowProcess() ) )
        {
        collector.add( new Tuple( i ) );
        }
      }

    S3Tap tap = new S3Tap( textDelimited, s3Rule.get3Client(), bucketName, key );
    String[] childIdentifiers = tap.getChildIdentifiers( FlowProcess.nullFlowProcess(), Integer.MAX_VALUE, false );

    assertEquals( totalItems, childIdentifiers.length );
    assertTrue( !childIdentifiers[ 0 ].startsWith( key ) );

    childIdentifiers = tap.getChildIdentifiers( FlowProcess.nullFlowProcess(), Integer.MAX_VALUE, true );

    assertNotSame( 0, childIdentifiers.length );
    assertTrue( childIdentifiers[ 0 ].contains( key ) );

    int sum = TupleStream.tupleStream( tap, FlowProcess.nullFlowProcess() )
      .mapToInt( t -> t.getInteger( 0 ) )
      .sum();

    assertEquals( IntStream.range( 0, totalItems ).sum(), sum );
    }

  @Test
  public void writeReadPartitioned() throws Exception
    {
    String key = "write-read-partitioned/";
    TextDelimited textDelimited = new TextDelimited( new Fields( "char", "value" ).applyTypes( String.class, int.class ) );

    int totalItems = 100;

    for( int i = 0; i < totalItems; i++ )
      {
      Tap output = new S3Tap( textDelimited, s3Rule.get3Client(), bucketName, key + i );

      output = new PartitionTap( output, new DelimitedPartition( new Fields( "char" ), "/" ) );

      try( TupleEntryCollector collector = output.openForWrite( FlowProcess.nullFlowProcess() ) )
        {
        collector.add( new Tuple( Character.toString( (char) ( 'a' + i ) ), i ) );
        }
      }

    S3Tap tap = new S3Tap( textDelimited, s3Rule.get3Client(), bucketName, key );
    String[] childIdentifiers = tap.getChildIdentifiers( FlowProcess.nullFlowProcess(), Integer.MAX_VALUE, false );

    assertEquals( totalItems, childIdentifiers.length );
    assertTrue( !childIdentifiers[ 0 ].startsWith( key ) );

    childIdentifiers = tap.getChildIdentifiers( FlowProcess.nullFlowProcess(), Integer.MAX_VALUE, true );

    assertNotSame( 0, childIdentifiers.length );
    assertTrue( childIdentifiers[ 0 ].contains( key ) );

    int sum = TupleStream.tupleStream( tap, FlowProcess.nullFlowProcess() )
      .mapToInt( t -> t.getInteger( 1 ) )
      .sum();

    assertEquals( IntStream.range( 0, totalItems ).sum(), sum );
    }

  @Test
  public void readS3Glob() throws Exception
    {
    String key = "glob/";
    TextLine textLine = new TextLine( new Fields( "line" ) );
    TextLine jsonLine = new TextLine( new Fields( "line" ) )
      {
      @Override
      protected String getBaseFileExtension()
        {
        return "json";
        }
      };

    for( int i = 0; i < 100; i++ )
      {
      S3Tap output = new S3Tap( ( ( i % 2 ) == 0 ? textLine : jsonLine ), s3Rule.get3Client(), bucketName, key + "foo/bar/" + i );

      try( TupleEntryCollector collector = output.openForWrite( FlowProcess.nullFlowProcess() ) )
        {
        collector.add( new Tuple( i ) );
        }
      }

    S3Tap tap = new S3Tap( textLine, s3Rule.get3Client(), S3Tap.makeURI( bucketName, key, "**/*.txt" ) );
    String[] childIdentifiers = tap.getChildIdentifiers( FlowProcess.nullFlowProcess(), Integer.MAX_VALUE, true );

    assertEquals( 50, childIdentifiers.length );

    for( String childIdentifier : childIdentifiers )
      {
      assertTrue( childIdentifier.contains( "/foo/bar/" ) );
      assertTrue( childIdentifier.endsWith( ".txt" ) );
      }
    }

  public static class TestS3Checkpoint implements S3Checkpointer
    {
    public String key;
    public boolean committed = false;

    public TestS3Checkpoint( String key )
      {
      this.key = key;
      }

    @Override
    public String getLastKey( String bucketName )
      {
      return key;
      }

    @Override
    public void setLastKey( String bucketName, String key )
      {
      this.key = key;
      }

    @Override
    public void commit()
      {
      committed = true;
      }
    }

  @Test
  public void writeReadMark() throws Exception
    {
    String key = "write-read-checkpoint/";
    TextDelimited textLine = new TextDelimited( new Fields( "value", int.class ) );

    int totalItems = 100;

    for( int i = 0; i < totalItems; i++ )
      {
      S3Tap output = new S3Tap( textLine, s3Rule.get3Client(), bucketName, String.format( "%s%04d", key, i ) );

      try( TupleEntryCollector collector = output.openForWrite( FlowProcess.nullFlowProcess() ) )
        {
        collector.add( new Tuple( i ) );
        }
      }

    TestS3Checkpoint checkpoint = new TestS3Checkpoint( "write-read-checkpoint/0050.txt" );

    S3Tap tap = new S3Tap( textLine, s3Rule.get3Client(), checkpoint, bucketName, key );
    String[] childIdentifiers = tap.getChildIdentifiers( FlowProcess.nullFlowProcess(), Integer.MAX_VALUE, false );

    assertEquals( 49, childIdentifiers.length );
    assertTrue( !childIdentifiers[ 0 ].startsWith( key ) );

    try( Stream<Tuple> tupleStream = TupleStream.tupleStream( tap, FlowProcess.nullFlowProcess() ) )
      {
      int sum = tupleStream
        .mapToInt( t -> t.getInteger( 0 ) )
        .sum();

      assertEquals( IntStream.range( 51, 100 ).sum(), sum );
      }

    assertEquals( "write-read-checkpoint/0099.tsv", checkpoint.key );
    assertTrue( checkpoint.committed );
    }

  @Test
  public void writeReadMarkOnDisk() throws Exception
    {
    String key = "write-read-checkpoint-disk/";
    TextDelimited textLine = new TextDelimited( new Fields( "value", int.class ) );

    int totalItems = 100;

    for( int i = 0; i < totalItems; i++ )
      {
      S3Tap output = new S3Tap( textLine, s3Rule.get3Client(), bucketName, String.format( "%s%04d", key, i ) );

      try( TupleEntryCollector collector = output.openForWrite( FlowProcess.nullFlowProcess() ) )
        {
        collector.add( new Tuple( i ) );
        }
      }

    Path path = Paths.get( getOutputPath() ).resolve( "checkpoints-" + SecureRandom.getInstanceStrong().nextInt() );

    {
    S3Tap tap = new S3Tap( textLine, s3Rule.get3Client(), new S3FileCheckpointer( path ), bucketName, key );

    try( Stream<Tuple> tupleStream = TupleStream.tupleStream( tap, FlowProcess.nullFlowProcess() ) )
      {
      int sum = tupleStream
        .limit( 10 )
        .mapToInt( t -> t.getInteger( 0 ) )
        .sum();

      assertEquals( IntStream.range( 0, 10 ).sum(), sum );
      }
    }

    {
    S3Tap tap = new S3Tap( textLine, s3Rule.get3Client(), new S3FileCheckpointer( path ), bucketName, key );

    try( Stream<Tuple> tupleStream = TupleStream.tupleStream( tap, FlowProcess.nullFlowProcess() ) )
      {
      int sum = tupleStream
        .mapToInt( t -> t.getInteger( 0 ) )
        .sum();

      assertEquals( IntStream.range( 10, 100 ).sum(), sum );
      }
    }
    }
  }
