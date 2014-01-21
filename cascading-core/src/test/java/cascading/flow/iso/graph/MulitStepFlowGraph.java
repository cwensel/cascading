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
import cascading.pipe.Checkpoint;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import static cascading.util.Util.createHashMap;

/**
 *
 */
public class MulitStepFlowGraph extends FlowElementGraph
  {
  public MulitStepFlowGraph()
    {
    Pipe lower = new Pipe( "lower" );
    Pipe upper = new Pipe( "upper" );

    lower = new Checkpoint( lower );
    upper = new Checkpoint( upper );

    lower = new Checkpoint( lower );
    upper = new Checkpoint( upper );

    Pipe sink = new Merge( "sink", lower, upper );

    Map<String, Tap> sources = createHashMap();

    sources.put( lower.getName(), new NonTap( new Fields( "offset", "line" ) ) );
    sources.put( upper.getName(), new NonTap( new Fields( "offset", "line" ) ) );

    Map<String, Tap> sinks = createHashMap();

    sinks.put( sink.getName(), new NonTap( new Fields( "offset", "line" ) ) );

    initialize( sources, sinks, sink );
    }
  }
