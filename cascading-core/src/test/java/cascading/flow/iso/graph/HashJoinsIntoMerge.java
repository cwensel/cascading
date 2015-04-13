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
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 *
 */
public class HashJoinsIntoMerge extends FlowElementGraph
  {
  public HashJoinsIntoMerge()
    {
    Map sources = new HashMap();
    sources.put( "lower", new NonTap( "lower", new Fields( "offset", "line" ) ) );
    sources.put( "upper", new NonTap( "upper", new Fields( "offset", "line" ) ) );
    sources.put( "lhs", new NonTap( "lhs", new Fields( "offset", "line" ) ) );
    sources.put( "rhs", new NonTap( "rhs", new Fields( "offset", "line" ) ) );


    Map sinks = new HashMap();
    sinks.put( "sink", new NonTap( "sink", new Fields( "offset", "line" ) ) );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe pipeLhs = new Each( new Pipe( "lhs" ), new Fields( "line" ), splitter );
    Pipe pipeRhs = new Each( new Pipe( "rhs" ), new Fields( "line" ), splitter );

    Pipe upperLower = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    upperLower = new Each( upperLower, new Identity() );

    Pipe lhsRhs = new HashJoin( pipeLhs, new Fields( "num" ), pipeRhs, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    lhsRhs = new Each( lhsRhs, new Identity() );

    Pipe merge = new Merge( "sink", Pipe.pipes( upperLower, lhsRhs ) );

    initialize( sources, sinks, merge );
    }
  }
