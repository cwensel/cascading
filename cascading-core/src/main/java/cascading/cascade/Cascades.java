/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.cascade;

import java.util.HashMap;
import java.util.Map;

import cascading.cascade.planner.FlowGraph;
import cascading.cascade.planner.IdentifierGraph;
import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

/**
 * Class Cascades provides useful utilities for use in constructing {@link Cascade} and {@link cascading.flow.Flow}
 * instances via the {@link CascadeConnector} and {@link cascading.flow.FlowConnector}, respectively.
 * <p/>
 * See the {@link FlowDef} for the recommended alternative to dealing with Maps of Taps.
 */
public class Cascades
  {
  /**
   * Method tapsMap creates a new Map for the given name and tap.
   *
   * @param name of type String
   * @param tap  of type Tap
   * @return Map
   */
  public static Map<String, Tap> tapsMap( String name, Tap tap )
    {
    return tapsMap( new String[]{name}, Tap.taps( tap ) );
    }

  /**
   * Method tapsMap creates a new Map for each name and tap.
   *
   * @param names of type String[]
   * @param taps  of type Tap[]
   * @return Map
   */
  public static Map<String, Tap> tapsMap( String[] names, Tap[] taps )
    {
    Map<String, Tap> map = new HashMap<String, Tap>();

    for( int i = 0; i < names.length; i++ )
      map.put( names[ i ], taps[ i ] );

    return map;
    }

  /**
   * Method tapsMap creates a new Map using the given Pipe name and tap.
   *
   * @param pipe of type Pipe
   * @param tap  of type Tap
   * @return Map
   */
  public static Map<String, Tap> tapsMap( Pipe pipe, Tap tap )
    {
    return tapsMap( Pipe.pipes( pipe ), Tap.taps( tap ) );
    }

  /**
   * Method tapsMap creates a new Map using the given pipes and taps.
   *
   * @param pipes of type Pipe[]
   * @param taps  of type Tap[]
   * @return Map
   */
  public static Map<String, Tap> tapsMap( Pipe[] pipes, Tap[] taps )
    {
    Map<String, Tap> map = new HashMap<String, Tap>();

    for( int i = 0; i < pipes.length; i++ )
      map.put( pipes[ i ].getName(), taps[ i ] );

    return map;
    }

  public static FlowGraph getFlowGraphFrom( Cascade cascade )
    {
    return cascade.getFlowGraph();
    }

  public static IdentifierGraph getTapGraphFrom( Cascade cascade )
    {
    return cascade.getIdentifierGraph();
    }
  }
