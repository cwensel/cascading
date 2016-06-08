/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;

/**
 *
 */
public class HashJoinMergeIntoHashJoinStreamedStreamedMergeGraph extends FlowElementGraph
  {
  public HashJoinMergeIntoHashJoinStreamedStreamedMergeGraph()
    {
    Map sources = new HashMap();
    sources.put( "lower", new NonTap( "lower", new Fields( "offset", "line" ) ) );
    sources.put( "upper", new NonTap( "upper", new Fields( "offset", "line" ) ) );
    sources.put( "offset", new NonTap( "offset", new Fields( "offset", "line" ) ) );

    Map sinks = new HashMap();
    sinks.put( "sink", new NonTap( "sink", new Fields( "offset", "line" ) ) );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), new RegexSplitter( new Fields( "num2", "char2" ), " " ) );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );

    splice = new Retain( splice, new Fields( "num1", "char1" ) );

    splice = new Merge( "merge1", splice, pipeUpper );

    splice = new HashJoin( splice, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );

    splice = new Retain( splice, new Fields( "num1", "char1" ) );

    splice = new Merge( "merge2", splice, pipeUpper );

    splice = new HashJoin( splice, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );

    splice = new Pipe( "sink", splice );

    initialize( sources, sinks, splice );
    }
  }
