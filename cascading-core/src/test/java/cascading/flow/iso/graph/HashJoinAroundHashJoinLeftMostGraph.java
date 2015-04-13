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
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 *
 */
public class HashJoinAroundHashJoinLeftMostGraph extends FlowElementGraph
  {
  public HashJoinAroundHashJoinLeftMostGraph()
    {
    Map sources = new HashMap();
    sources.put( "lower", new NonTap( "lower", new Fields( "offset", "line" ) ) );
    sources.put( "upper1", new NonTap( "upper", new Fields( "offset", "line" ) ) );
    sources.put( "upper2", new NonTap( "upper", new Fields( "offset", "line" ) ) );


    Map sinks = new HashMap();
    sinks.put( "sink", new NonTap( "sink", new Fields( "offset", "line" ) ) );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper1 = new Each( new Pipe( "upper1" ), new Fields( "line" ), splitter );
    Pipe pipeUpper2 = new Each( new Pipe( "upper2" ), new Fields( "line" ), splitter );

    Pipe splice1 = new HashJoin( pipeUpper1, new Fields( "num" ), pipeUpper2, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    splice1 = new Each( splice1, new Identity() );

    Pipe splice2 = new HashJoin( "sink", splice1, new Fields( "num1" ), pipeLower, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    initialize( sources, sinks, splice2 );
    }
  }
