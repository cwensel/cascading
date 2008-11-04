/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe.cogroup;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import cascading.tuple.Tuple;

/**
 * Class OuterJoin will return an {@link Iterator} that will iterate over a given {@link Joiner} and return tuples that represent
 * and outer join of the CoGrouper internal grouped tuple collections.
 */
public class OuterJoin implements Joiner
  {
  public Iterator<Tuple> getIterator( GroupClosure closure )
    {
    return new JoinIterator( closure );
    }

  public int numJoins()
    {
    return -1;
    }

  protected static class JoinIterator extends InnerJoin.JoinIterator
    {
    List[] singletons;

    public JoinIterator( GroupClosure closure )
      {
      super( closure );
      }

    protected final CoGroupClosure getCoGroupClosure()
      {
      return (CoGroupClosure) closure;
      }

    @Override
    protected void init()
      {
      singletons = new List[closure.size()];

      for( int i = 0; i < singletons.length; i++ )
        {
        if( isOuter( i ) )
          singletons[ i ] = Collections.singletonList( Tuple.size( closure.valueFields[ i ].size() ) );
        }

      super.init();
      }

    protected boolean isOuter( int i )
      {
      return getCoGroupClosure().getGroup( i ).size() == 0;
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
