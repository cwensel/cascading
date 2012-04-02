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

package cascading.tuple.hadoop;

import java.util.Collection;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.tuple.SpillableTupleList;
import cascading.tuple.SpillableTupleMap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleMapFactory;

/**
 *
 */
public class HadoopTupleMapFactory implements TupleMapFactory
  {
  private int capacity;
  private float loadFactor;
  private int mapThreshold;
  private int listThreshold;

  @Override
  public void initialize( FlowProcess flowProcess )
    {
    capacity = SpillableTupleMap.getMapCapacity( flowProcess, SpillableTupleMap.initialCapacity );
    loadFactor = SpillableTupleMap.getMapLoadFactor( flowProcess, SpillableTupleMap.loadFactor );
    mapThreshold = SpillableTupleMap.getMapThreshold( flowProcess, SpillableTupleMap.defaultThreshold );
    listThreshold = SpillableTupleList.getThreshold( flowProcess, SpillableTupleList.defaultThreshold );
    }

  @Override
  public Map<Tuple, Collection<Tuple>> create( FlowProcess flowProcess )
    {
    return new HadoopSpillableTupleMap( capacity, loadFactor, mapThreshold, listThreshold, flowProcess );
    }
  }
