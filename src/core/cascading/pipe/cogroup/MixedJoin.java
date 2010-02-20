/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import java.util.Arrays;
import java.util.Iterator;

import cascading.tuple.Tuple;

/**
 * Class MixedJoin will return an {@link java.util.Iterator} that will iterate over a given
 * {@link Joiner} and return tuples that represent a join as defined by the given boolean array.
 * <p/>
 * So if joining three streams, {@code boolean []{true,false,false}} will result in a 'inner', 'outer', 'outer' join.
 */
public class MixedJoin implements Joiner
  {
  /** Field INNER */
  public static boolean INNER = true;
  /** Field OUTER */
  public static boolean OUTER = false;

  boolean asInner[];

  /**
   * Constructor MixedJoin creates a new MixedJoin instance.
   *
   * @param asInner of type boolean[]
   */
  public MixedJoin( boolean[] asInner )
    {
    this.asInner = Arrays.copyOf( asInner, asInner.length );
    }

  /** @see Joiner#numJoins() */
  public int numJoins()
    {
    return asInner.length - 1;
    }

  public Iterator<Tuple> getIterator( GroupClosure closure )
    {
    return new JoinIterator( closure );
    }

  protected class JoinIterator extends OuterJoin.JoinIterator
    {
    public JoinIterator( GroupClosure closure )
      {
      super( closure );
      }

    @Override
    protected boolean isOuter( int i )
      {
      return !asInner[ i ] && super.isOuter( i );
      }
    }
  }