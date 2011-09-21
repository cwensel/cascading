/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow;

import cascading.PlatformTestCase;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;

import static data.InputData.inputFileApache;

@PlatformTest(platforms = {"local", "hadoop"})
public class FlowSkipTest extends PlatformTestCase
  {
  public FlowSkipTest()
    {
    super( false ); // leave cluster testing disabled
    }

  public void testSkipStrategiesReplace() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    // !!! enable replace
    Tap sink = getPlatform().getTextFile( getOutputPath( "replace" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    sink.deleteResource( flow.getConfig() );

    assertTrue( "default skip", !flow.getFlowSkipStrategy().skipFlow( flow ) );
    assertTrue( "exist skip", !new FlowSkipIfSinkExists().skipFlow( flow ) );

    flow.complete();

    assertTrue( "default skip", !flow.getFlowSkipStrategy().skipFlow( flow ) );
    assertTrue( "exist skip", !new FlowSkipIfSinkExists().skipFlow( flow ) );

    FlowSkipStrategy old = flow.getFlowSkipStrategy();

    FlowSkipStrategy replaced = flow.setFlowSkipStrategy( new FlowSkipIfSinkExists() );

    assertTrue( "not same instance", old == replaced );

    validateLength( flow.openSource(), 10 ); // validate source, this once, as a sanity check
    validateLength( flow, 10, null );
    }

  public void testSkipStrategiesKeep() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    // !!! enable replace
    Tap sink = getPlatform().getTextFile( getOutputPath( "keep" ), SinkMode.KEEP );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    sink.deleteResource( flow.getConfig() );

    assertTrue( "default skip", !flow.getFlowSkipStrategy().skipFlow( flow ) );
    assertTrue( "exist skip", !new FlowSkipIfSinkExists().skipFlow( flow ) );

    flow.complete();

    assertTrue( "default skip", flow.getFlowSkipStrategy().skipFlow( flow ) );
    assertTrue( "exist skip", new FlowSkipIfSinkExists().skipFlow( flow ) );

    validateLength( flow.openSource(), 10 ); // validate source, this once, as a sanity check
    validateLength( flow, 10, null );
    }
  }