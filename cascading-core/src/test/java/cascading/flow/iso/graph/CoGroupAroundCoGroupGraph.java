/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 *
 */
public class CoGroupAroundCoGroupGraph extends FlowElementGraph
  {
  public CoGroupAroundCoGroupGraph()
    {
    Map sources = new HashMap();

    NonTap source10 = new NonTap( "source10", new Fields( "offset", "line" ) );
    NonTap source20 = new NonTap( "source20", new Fields( "offset", "line" ) );
    sources.put( "source20", source20 );
    sources.put( "source101", source10 );
    sources.put( "source102", source10 );

    Map sinks = new HashMap();
    sinks.put( "sink", new NonTap( "sink", new Fields( "offset", "line" ) ) );

    Pipe pipeNum20 = new Pipe( "source20" );
    Pipe pipeNum101 = new Pipe( "source101" );
    Pipe pipeNum102 = new Pipe( "source102" );

    Pipe splice1 = new CoGroup( pipeNum20, new Fields( "num" ), pipeNum101, new Fields( "num" ), new Fields( "num1", "num2" ) );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeNum102, new Fields( "num" ), new Fields( "num1", "num2", "num3" ) );

    splice2 = new Each( splice2, new Identity() );

    splice2 = new Pipe( "sink", splice2 );

    initialize( sources, sinks, splice2 );
    }
  }
