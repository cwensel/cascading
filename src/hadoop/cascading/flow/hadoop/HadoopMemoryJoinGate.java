/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.flow.stream.MemoryJoinGate;
import cascading.pipe.Join;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.HadoopSpillableTupleMap;

/**
 *
 */
public class HadoopMemoryJoinGate extends MemoryJoinGate
  {
  public HadoopMemoryJoinGate( FlowProcess flowProcess, Join join )
    {
    super( flowProcess, join );
    }

  @Override
  protected Set<Tuple> createKeySet()
    {
    return new HashSet<Tuple>(); // does not need to be synchronized, or ordered
    }

  @Override
  protected Map<Tuple, Collection<Tuple>> createTupleMap()
    {
    return new HadoopSpillableTupleMap( 1000, 1000, flowProcess );
    }

  @Override
  protected void waitOnLatch()
    {
    // do nothing
    }

  @Override
  protected void countDownLatch()
    {
    // do nothing
    }
  }
