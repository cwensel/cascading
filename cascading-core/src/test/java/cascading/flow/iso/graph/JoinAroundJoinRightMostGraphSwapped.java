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
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import static cascading.util.Util.createHashMap;

/**
 *
 */
public class JoinAroundJoinRightMostGraphSwapped extends FlowElementGraph
  {
  public JoinAroundJoinRightMostGraphSwapped()
    {
    Function function = new Insert( new Fields( "num", "char" ), "a", "b" );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), function );
    Pipe pipeUpper1 = new Each( new Pipe( "upper1" ), new Fields( "line" ), function );
    Pipe pipeUpper2 = new Each( new Pipe( "upper2" ), new Fields( "line" ), function );

    Pipe splice1 = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper1, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    splice1 = new Each( splice1, new Identity() );

    Pipe splice2 = new HashJoin( pipeUpper2, new Fields( "num1" ), splice1, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    Map<String, Tap> sources = createHashMap();

    sources.put( "lower", new NonTap( new Fields( "offset", "line" ) ) );

    NonTap shared = new NonTap( new Fields( "offset", "line" ) );
    sources.put( "upper1", shared );
    sources.put( "upper2", shared );

    Map<String, Tap> sinks = createHashMap();

    sinks.put( splice2.getName(), new NonTap( new Fields( "offset", "line" ) ) );

    initialize( sources, sinks, splice2 );
    }
  }
