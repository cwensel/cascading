/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

import java.util.Iterator;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class BufferJoiner notifies the Cascading planner that the next {@link cascading.operation.Buffer} instance
 * will implement a custom join strategy.
 * <p>
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
