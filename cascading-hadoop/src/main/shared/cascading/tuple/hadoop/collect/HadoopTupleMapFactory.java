/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop.collect;

import java.util.Collection;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.tuple.Tuple;
import cascading.tuple.collect.SpillableProps;
import cascading.tuple.collect.SpillableTupleList;
import cascading.tuple.collect.SpillableTupleMap;
import cascading.tuple.collect.TupleMapFactory;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class HadoopTupleMapFactory implements TupleMapFactory<JobConf>
  {
  private int capacity;
  private float loadFactor;
  private int mapThreshold;
  private int listThreshold;

  @Override
  public void initialize( FlowProcess<JobConf> flowProcess )
    {
    capacity = SpillableTupleMap.getMapCapacity( flowProcess, SpillableProps.defaultMapInitialCapacity );
    loadFactor = SpillableTupleMap.getMapLoadFactor( flowProcess, SpillableProps.defaultMapLoadFactor );
    mapThreshold = SpillableTupleMap.getMapThreshold( flowProcess, SpillableProps.defaultMapThreshold );
    listThreshold = SpillableTupleList.getThreshold( flowProcess, SpillableProps.defaultListThreshold );
    }

  @Override
  public Map<Tuple, Collection<Tuple>> create( FlowProcess<JobConf> flowProcess )
    {
    return new HadoopSpillableTupleMap( capacity, loadFactor, mapThreshold, listThreshold, flowProcess );
    }
  }
