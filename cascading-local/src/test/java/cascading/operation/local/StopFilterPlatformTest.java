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

package cascading.operation.local;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.StepCounters;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.filter.Limit;
import cascading.operation.filter.Stop;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.*;

/**
 *
 */
public class StopFilterPlatformTest extends PlatformTestCase
  {

  public StopFilterPlatformTest()
    {
    super( true, 5, 3 ); // leave cluster testing enabled
    }

  @Test
  public void testSimple() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache200 );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache200 );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Each( pipe, new Fields( "ip" ), new Stop( new Limit( 100 ) ) );

    Tap sink = getPlatform().getTextFile( getOutputPath(), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow.openSink(), 100 );
    assertEquals( 100, flow.getFlowStats().getCounterValue( StepCounters.Tuples_Written ) );
    assertEquals( 101, flow.getFlowStats().getCounterValue( StepCounters.Tuples_Read ) );
    }

  @Test
  public void testSimpleGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache200 );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache200 );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    pipe = new Each( pipe, new Fields( "ip" ), new Stop( new Limit( 100 ) ) );

    Tap sink = getPlatform().getTextFile( getOutputPath(), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow.openSink(), 100 );
    assertEquals( 100, flow.getFlowStats().getCounterValue( StepCounters.Tuples_Written ) );
    assertEquals( 200, flow.getFlowStats().getCounterValue( StepCounters.Tuples_Read ) );
    }

  @Test
  public void testCoGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "cogroup" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    pipeLower = new Each( pipeLower, new Fields( "num", "char" ), new Stop( new Limit( 2 ) ) );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    pipeUpper = new Each( pipeUpper, new Fields( "num", "char" ), new Stop( new Limit( 2 ) ) );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), new InnerJoin( Fields.size( 4 ) ) );

    splice = new Each( splice, Fields.ALL, new Stop( new Limit( 2 ) ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 2 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );

    assertEquals( 2, flow.getFlowStats().getCounterValue( StepCounters.Tuples_Written ) );
    assertEquals( 6, flow.getFlowStats().getCounterValue( StepCounters.Tuples_Read ) );
    }
  }
