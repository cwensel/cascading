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

import java.util.Map;

import cascading.flow.iso.NonFunction;
import cascading.flow.iso.NonTap;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import static cascading.util.Util.createHashMap;

/**
 *
 */
public class StandardElementGraph extends FlowElementGraph
  {
  public StandardElementGraph()
    {
    this( false );
    }

  public StandardElementGraph( boolean testBetween )
    {
    Function function = new NonFunction( new Fields( "num", "char" ) );

    Pipe pipeLower = new Each( "lower", new Fields( "line" ), function );
    Pipe pipeUpper1 = new Each( "upper1", new Fields( "line" ), function );
    Pipe pipeUpper2 = new Each( "upper2", new Fields( "line" ), function );

    Pipe splice1 = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper1, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    splice1 = new Each( splice1, new Identity() );
    splice1 = new Each( splice1, new Identity() );
    splice1 = new Each( new Pipe( "last", splice1 ), new Identity() );

    splice1 = new GroupBy( splice1, new Fields( 0 ) );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeUpper2, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    if( testBetween )
      splice2 = new Each( new Pipe( "before", splice2 ), new Identity() );

    splice2 = new Each( new Pipe( "remove", splice2 ), new Identity() );

    if( testBetween )
      splice2 = new Each( new Pipe( "after", splice2 ), new Identity() );

    splice2 = new GroupBy( splice2, new Fields( 0 ) );

    Pipe tail = new CoGroup( splice2, new Fields( "num1" ), splice1, new Fields( "num1" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3", "num4", "char4", "num5", "char5" ) );

    tail = new Each( tail, new Identity() );

    Pipe lhsTail = new Pipe( "lhsTail", tail );

    lhsTail = new Each( lhsTail, new Identity() );

    Pipe rhsTail = new Pipe( "rhsTail", tail );

    rhsTail = new Each( rhsTail, new Identity() );

    Map<String, Tap> sources = createHashMap();

    sources.put( "lower", new NonTap( new Fields( "offset", "line" ) ) );
    sources.put( "upper1", new NonTap( new Fields( "offset", "line" ) ) );
    sources.put( "upper2", new NonTap( new Fields( "offset", "line" ) ) );

    Map<String, Tap> sinks = createHashMap();

    sinks.put( lhsTail.getName(), new NonTap( new Fields( "offset", "line" ) ) );
    sinks.put( rhsTail.getName(), new NonTap( new Fields( "offset", "line" ) ) );

    initialize( sources, sinks, lhsTail, rhsTail );
    }
  }
