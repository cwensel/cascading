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

package cascading.pipe.joiner;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import cascading.tuple.Tuple;

/**
 * Class OuterJoin will return an {@link Iterator} that will iterate over a given {@link Joiner} and return tuples that represent
 * and outer join of the CoGrouper internal grouped tuple collections.
 * <p/>
 * Joins perform based on the equality of the join keys. In the case of null values, Java treats two
 * null values as equivalent. SQL does not treat null values as equal. To produce SQL like results in a given
 * join, a new {@link java.util.Comparator} will need to be used on the joined values to prevent null from
 * equaling null. As a convenience, see the {@link cascading.util.NullNotEquivalentComparator} class.
 */
public class OuterJoin implements Joiner
  {
  public Iterator<Tuple> getIterator( JoinerClosure closure )
    {
    return new JoinIterator( closure );
    }

  public int numJoins()
    {
    return -1;
    }

  public static class JoinIterator extends InnerJoin.JoinIterator
    {
    List[] singletons;

    public JoinIterator( JoinerClosure closure )
      {
      super( closure );
      }

    @Override
    protected void init()
      {
      singletons = new List[ closure.size() ];

      for( int i = 0; i < singletons.length; i++ )
        {
        if( isOuter( i ) )
          singletons[ i ] = Collections.singletonList( Tuple.size( closure.getValueFields()[ i ].size() ) );
        }

      super.init();
      }

    protected boolean isOuter( int i )
      {
      return closure.isEmpty( i );
      }

    @Override
    protected Iterator getIterator( int i )
      {
      if( singletons[ i ] == null ) // let init() decide
        return super.getIterator( i );

      return singletons[ i ].iterator();
      }
    }
  }
