/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.iso.graph;

import java.util.Map;

import cascading.flow.iso.NonTap;
import cascading.flow.planner.FlowElementGraph;
import cascading.operation.AssertionLevel;
import cascading.operation.assertion.AssertGroupSizeEquals;
import cascading.operation.assertion.AssertMatches;
import cascading.operation.assertion.AssertNotNull;
import cascading.operation.regex.RegexFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import static cascading.util.Util.createHashMap;

/**
 *
 */
public class LoneGroupAssertionGraph extends FlowElementGraph
  {
  public LoneGroupAssertionGraph()
    {
    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, AssertionLevel.STRICT, new AssertNotNull() );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "line" ), AssertionLevel.STRICT, new AssertMatches( "^POST" ) );

    pipe = new GroupBy( pipe, new Fields( "line" ) );

    pipe = new Every( pipe, AssertionLevel.STRICT, new AssertGroupSizeEquals( 7L ) );

    Map<String, Tap> sources = createHashMap();

    sources.put( "test", new NonTap( new Fields( "line" ) ) );

    Map<String, Tap> sinks = createHashMap();

    sinks.put( "test", new NonTap() );

    initialize( sources, sinks, pipe );
    }
  }
