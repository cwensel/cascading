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

import java.util.Arrays;
import java.util.Iterator;

import cascading.tuple.Tuple;

/**
 * Class MixedJoin will return an {@link java.util.Iterator} that will iterate over a given
 * {@link Joiner} and return tuples that represent a join as defined by the given boolean array.
 * <p/>
 * So if joining three streams, {@code boolean []{true,false,false}} will result in a 'inner', 'outer', 'outer' join.
 * <p/>
 * Joins perform based on the equality of the join keys. In the case of null values, Java treats two
 * null values as equivalent. SQL does not treat null values as equal. To produce SQL like results in a given
 * join, a new {@link java.util.Comparator} will need to be used on the joined values to prevent null from
 * equaling null. As a convenience, see the {@link cascading.util.NullNotEquivalentComparator} class.
 */
public class MixedJoin implements Joiner
  {
  /** Field INNER */
  public static boolean INNER = true;
  /** Field OUTER */
  public static boolean OUTER = false;

  final boolean[] asInner;

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

  public Iterator<Tuple> getIterator( JoinerClosure closure )
    {
    return new JoinIterator( closure );
    }

  public class JoinIterator extends OuterJoin.JoinIterator
    {
    public JoinIterator( JoinerClosure closure )
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