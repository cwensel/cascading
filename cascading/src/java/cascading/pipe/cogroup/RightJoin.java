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

import java.util.Iterator;

import cascading.tuple.Tuple;

/**
 * Class RightJoin will return an {@link Iterator} that will iterate over a given {@link CoGrouper} and return tuples that represent
 * a left outer, right inner join of the CoGrouper internal grouped tuple collections.
 * <p/>
 * Note only the farthest left tuple stream will be used as the outer join. All following joins to the right will
 * be inner joins. See {@link cascading.pipe.cogroup.MixedJoin} for more flexibility.
 *
 * @see cascading.pipe.cogroup.MixedJoin
 */
public class RightJoin implements CoGrouper
  {

  public Iterator<Tuple> getIterator( GroupClosure closure )
    {
    return new JoinIterator( closure );
    }

  public int numJoins()
    {
    return -1;
    }

  protected static class JoinIterator extends OuterJoin.JoinIterator
    {

    public JoinIterator( GroupClosure closure )
      {
      super( closure );
      }

    @Override
    protected boolean isOuter( int i )
      {
      return i == 0 && super.isOuter( i );
      }
    }

  }
