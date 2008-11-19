/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.filter.And;
import cascading.operation.function.UnGroup;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.regex.Regexes;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.InnerJoin;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.MultiTap;
import cascading.tap.Tap;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleIterator;
import cascading.ClusterTestCase;
import cascading.TestAggregator;

/** @version $Id: //depot/calku/cascading/src/test/cascading/FieldedPipesTest.java#4 $ */
public class FlowSkipTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.10.txt";

  String outputPath = "build/test/output/flowskip/";

  public FlowSkipTest()
    {
    super( "flow skip", false ); // leave cluster testing disabled
    }

  public void testSkipStrategiesReplace() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    // !!! enable replace
    Tap sink = new Hfs( new TextLine(), outputPath + "/replace", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    sink.deletePath( flow.getJobConf() );

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
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    // !!! enable replace
    Tap sink = new Hfs( new TextLine(), outputPath + "/keep", SinkMode.KEEP );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    sink.deletePath( flow.getJobConf() );

    assertTrue( "default skip", !flow.getFlowSkipStrategy().skipFlow( flow ) );
    assertTrue( "exist skip", !new FlowSkipIfSinkExists().skipFlow( flow ) );

    flow.complete();

    assertTrue( "default skip", flow.getFlowSkipStrategy().skipFlow( flow ) );
    assertTrue( "exist skip", new FlowSkipIfSinkExists().skipFlow( flow ) );

    validateLength( flow.openSource(), 10 ); // validate source, this once, as a sanity check
    validateLength( flow, 10, null );
    }
  }