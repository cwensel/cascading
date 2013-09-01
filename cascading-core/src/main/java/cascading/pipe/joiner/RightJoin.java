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

import java.beans.ConstructorProperties;
import java.util.Iterator;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class RightJoin will return an {@link Iterator} that will iterate over a given {@link Joiner} and return tuples that represent
 * a left outer, right inner join of the CoGrouper internal grouped tuple collections.
 * <p/>
 * Note only the farthest left tuple stream will be used as the outer join. All following joins to the right will
 * be inner joins. See {@link MixedJoin} for more flexibility.
 *
 * @see MixedJoin
 */
public class RightJoin extends BaseJoiner
  {
  public RightJoin()
    {
    }

  @ConstructorProperties({"fieldDeclaration"})
  public RightJoin( Fields fieldDeclaration )
    {
    super( fieldDeclaration );
    }

  public Iterator<Tuple> getIterator( JoinerClosure closure )
    {
    return new JoinIterator( closure );
    }

  public int numJoins()
    {
    return -1;
    }

  public static class JoinIterator extends OuterJoin.JoinIterator
    {
    public JoinIterator( JoinerClosure closure )
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
