/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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

package cascading.flow.local;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

import static cascading.tuple.TupleEntryStream.entryStream;
import static data.InputData.inputFileApache200;

/**
 *
 */
public class FlowPlatformTest extends PlatformTestCase
  {
  /**
   * This test confirms the flow#stop() call will properly shutdown the threading and gracefully commit
   * any pending data.
   */
  @Test
  public void testStop() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache200 );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache200 );

    Pipe pipe = new Pipe( "test" );

    int limit = 10;
    Semaphore start = new Semaphore( 0 );
    AtomicInteger count = new AtomicInteger( 0 );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( Fields.ARGS )
      {
      @Override
      public void operate( FlowProcess flowProcess, FunctionCall<Identity.Functor> functionCall )
        {
        int i = count.getAndAdd( 1 );

        // want to confirm a close and flush actually happened in the tests below
        // so limiting the number of records through as it takes time for the call thread to shutdown
        if( i >= limit )
          {
//          System.out.println( "hit limit" );
          start.release();
          return;
          }

        super.operate( flowProcess, functionCall );
        }
      } );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simple" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

//    System.out.println( "starting flow" );
    flow.start();

//    System.out.println( "acquiring permit" );
    start.acquire();

//    System.out.println( "stopping flow" );
    flow.stop();

    long sourceSize = entryStream( source, flow.getFlowProcess() ).count();
    long sinkSize = entryStream( sink, flow.getFlowProcess() ).count();

    assertNotSame( sourceSize, sinkSize );
    assertEquals( limit, sinkSize );
    }
  }
