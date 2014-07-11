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

import java.util.HashMap;
import java.util.Map;

import cascading.flow.iso.NonTap;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.operation.Function;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 *
 */
public class SelfCoGroupGraph extends FlowElementGraph
  {
  public SelfCoGroupGraph()
    {
    Map sources = new HashMap();

    NonTap sourceLower = new NonTap( "lower", new Fields( "offset", "line" ) );
    sources.put( "lower", sourceLower );

    Map sinks = new HashMap();
    sinks.put( "sink", new NonTap( "sink", new Fields( "offset", "line" ) ) );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe lower = new Pipe( "lower" );

    Pipe pipeLower = new Each( new Pipe( "lhs", lower ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "rhs", lower ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( "sink", pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    initialize( sources, sinks, splice );
    }
  }
