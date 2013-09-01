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

package cascading.pipe.joiner;

import java.util.Iterator;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class BufferJoiner notifies the Cascading planner that the next {@link cascading.operation.Buffer} instance
 * will implement a custom join strategy.
 * <p/>
 * Internally this class simply returns {@link Fields#NONE} from {@link #getFieldDeclaration()} as a flag to
 * the planner.
 */
public class BufferJoin extends BaseJoiner
  {
  public BufferJoin()
    {
    super( Fields.NONE );
    }

  @Override
  public Iterator<Tuple> getIterator( JoinerClosure closure )
    {
    return null;
    }

  @Override
  public int numJoins()
    {
    return -1;
    }
  }
