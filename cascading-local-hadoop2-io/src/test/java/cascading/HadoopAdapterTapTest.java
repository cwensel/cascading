/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.platform.PlatformRunner;
import cascading.platform.hadoop2.Hadoop2MR1Platform;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tap.local.hadoop.LocalHfsAdaptor;
import cascading.tuple.Fields;
import org.junit.Test;

import static data.InputData.inputFileApache;

/**
 *
 */
@PlatformRunner.Platform({Hadoop2MR1Platform.class})
public class HadoopAdapterTapTest extends PlatformTestCase
  {
  public HadoopAdapterTapTest()
    {
    super( true, 5, 3 ); // leave cluster testing enabled
    }

  @Test
  public void testWriteReadHDFS() throws Exception
    {
    Tap source = new FileTap( new cascading.scheme.local.TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap intermediate = new LocalHfsAdaptor( new Hfs( new cascading.scheme.hadoop.TextLine(), getOutputPath( "/intermediate" ), SinkMode.REPLACE ) );
    Tap sink = new FileTap( new cascading.scheme.local.TextLine(), getOutputPath( "/final" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Flow first = new LocalFlowConnector( getPlatform().getProperties() ).connect( source, intermediate, pipe );

    first.complete();

    validateLength( first, 10 );

    Flow second = new LocalFlowConnector( getPlatform().getProperties() ).connect( intermediate, sink, pipe );

    second.complete();

    validateLength( second, 10 );
    }
  }
