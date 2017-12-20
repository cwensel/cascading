/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.cascade;

import java.io.IOException;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.util.Util;
import org.junit.Test;

/**
 *
 */
public class CascadeStalePlatformTest extends PlatformTestCase
  {
  public CascadeStalePlatformTest()
    {
    super( true );
    }

  interface TapSupplier
    {
    Tap supply( SinkMode mode );
    }

  @Test
  public void testCascadeSkipOnModifiedTime() throws IOException
    {
    final String outputPath = getOutputPath( "output" );

    runCascade( new TapSupplier()
      {
      @Override
      public Tap supply( SinkMode mode )
        {
        return getPlatform().getDelimitedFile( new Fields( "upper" ), "+", outputPath, mode );
        }
      } );
    }

  @Test
  public void testCascadePartitionSkipOnModifiedTime() throws IOException
    {
    final String outputPath = getOutputPath( "output" );

    runCascade( new TapSupplier()
      {
      @Override
      public Tap supply( SinkMode mode )
        {
        Tap partitionTap = getPlatform().getDelimitedFile( new Fields( "upper" ), "+", outputPath, mode );
        Partition partition = new DelimitedPartition( new Fields( "lower", "number" ) );

        partitionTap = getPlatform().getPartitionTap( partitionTap, partition, 1 );

        return partitionTap;
        }
      } );
    }

  private void runCascade( TapSupplier supplier ) throws IOException
    {
    String inputPath = getOutputPath( "input.txt" );

    Tap source = getPlatform().getDelimitedFile( new Fields( "number", "lower", "upper" ), " ", inputPath );

    TupleEntryCollector collector = source.openForWrite( getPlatform().getFlowProcess() );

    collector.add( new Tuple( 0, "a", "B" ) );
    collector.add( new Tuple( 1, "a", "B" ) );
    collector.add( new Tuple( 2, "a", "B" ) );

    collector.close();

    Tap sinkTap = supplier.supply( SinkMode.REPLACE );

    Flow firstFlow = getPlatform().getFlowConnector().connect( "first", source, sinkTap, new Pipe( "copy" ) );

    firstFlow.complete();

    System.out.println( "sinkTap.getModifiedTime = " + sinkTap.getModifiedTime( firstFlow.getFlowProcess() ) );

    sinkTap = supplier.supply( SinkMode.KEEP );

    Flow secondFlow = getPlatform().getFlowConnector().connect( "second", source, sinkTap, new Pipe( "copy" ) );

    Cascade firstCascade = new CascadeConnector().connect( secondFlow );

    firstCascade.complete();

    System.out.println( "sinkTap.getModifiedTime = " + sinkTap.getModifiedTime( secondFlow.getFlowProcess() ) );

    assertTrue( secondFlow.getStats().isSkipped() );

    assertTrue( "unable to delete resource", source.deleteResource( secondFlow.getFlowProcess() ) );

    Util.safeSleep( 1000 ); // be safe, delay execution

    collector = source.openForWrite( getPlatform().getFlowProcess() );

    collector.add( new Tuple( 0, "a", "B" ) );
    collector.add( new Tuple( 1, "a", "B" ) );
    collector.add( new Tuple( 2, "a", "B" ) );

    collector.close();

    sinkTap = supplier.supply( SinkMode.KEEP );

    Flow thirdFlow = getPlatform().getFlowConnector().connect( "third", source, sinkTap, new Pipe( "copy" ) );

    Cascade secondCascade = new CascadeConnector().connect( thirdFlow );

    secondCascade.complete();

    assertFalse( thirdFlow.getStats().isSkipped() );
    }
  }
