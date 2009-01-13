/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.cascade;

import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

/**
 * Class Cascades provides useful utilities for use in constructing {@link Cascade} and {@link Flow}
 * instances via the {@link CascadeConnector} and {@link FlowConnector}, respectively.
 */
public class Cascades
  {
  /**
   * Method tapsMap creates a new Map for the given name and tap.
   *
   * @param name of type String
   * @param tap  of type Tap
   * @return Map<String, Tap>
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
   * @return Map<String, Tap>
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
   * @return Map<String, Tap>
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
   * @return Map<String, Tap>
   */
  public static Map<String, Tap> tapsMap( Pipe[] pipes, Tap[] taps )
    {
    Map<String, Tap> map = new HashMap<String, Tap>();

    for( int i = 0; i < pipes.length; i++ )
      map.put( pipes[ i ].getName(), taps[ i ] );

    return map;
    }
  }
