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

package cascading.flow;

import java.util.TreeMap;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;

/**
 *
 */
public class FlowElements
  {
  public static String id( FlowElement flowElement )
    {
    if( flowElement instanceof Pipe )
      return Pipe.id( (Pipe) flowElement );

    if( flowElement instanceof Tap )
      return Tap.id( (Tap) flowElement );

    throw new IllegalArgumentException( "id not supported for: " + flowElement.getClass().getCanonicalName() );
    }

  public static int isPrevious( Pipe pipe, Pipe previous )
    {
    if( pipe == previous )
      return 0;

    if( pipe instanceof SubAssembly )
      {
      Pipe[] unwind = SubAssembly.unwind( pipe );

      for( Pipe unwound : unwind )
        {
        int result = collectPipes( unwound, 0, previous );

        if( result != -1 )
          return result;
        }

      return -1;
      }

    return collectPipes( pipe, 0, previous );
    }

  private static int collectPipes( Pipe pipe, int depth, Pipe... allPrevious )
    {
    depth++;

    for( Pipe previous : allPrevious )
      {
      if( pipe == previous )
        return depth;

      int result;
      if( previous instanceof SubAssembly )
        result = collectPipes( pipe, depth, SubAssembly.unwind( previous ) );
      else
        result = collectPipes( pipe, depth, previous.getPrevious() );

      if( result != -1 )
        return result;
      }

    return -1;
    }

  public static Integer findOrdinal( Pipe pipe, Pipe previous )
    {
    Pipe[] previousPipes = pipe.getPrevious();

    TreeMap<Integer, Integer> sorted = new TreeMap<>();

    for( int i = 0; i < previousPipes.length; i++ )
      {
      int result = isPrevious( previousPipes[ i ], (Pipe) previous );

      if( result == -1 )
        continue;

      sorted.put( result, i );
      }

    return sorted.firstEntry().getValue();
    }
  }
