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

package cascading.tuple.hadoop.collect;

import java.util.Collection;

import cascading.flow.FlowProcess;
import cascading.provider.FactoryLoader;
import cascading.tuple.Tuple;
import cascading.tuple.collect.Spillable;
import cascading.tuple.collect.SpillableTupleList;
import cascading.tuple.collect.SpillableTupleMap;
import cascading.tuple.collect.TupleCollectionFactory;
import cascading.tuple.collect.TupleMapFactory;
import org.apache.hadoop.mapred.JobConf;

/**
 * HadoopSpillableTupleMap is responsible for spilling values to disk if the map threshold is reached.
 *
 * @see SpillableTupleMap
 * @see SpillableTupleList
 */
public class HadoopSpillableTupleMap extends SpillableTupleMap
  {
  private final FlowProcess<JobConf> flowProcess;
  private final Spillable.SpillStrategy spillStrategy;
  private final TupleCollectionFactory<JobConf> tupleCollectionFactory;

  public HadoopSpillableTupleMap( int initialCapacity, float loadFactor, int mapThreshold, int listThreshold, FlowProcess<JobConf> flowProcess )
    {
    super( initialCapacity, loadFactor, mapThreshold, listThreshold );
    this.flowProcess = flowProcess;
    this.spillStrategy = getSpillStrategy();

    FactoryLoader loader = FactoryLoader.getInstance();

    this.tupleCollectionFactory = loader.loadFactoryFrom( flowProcess, TupleMapFactory.TUPLE_MAP_FACTORY, HadoopTupleCollectionFactory.class );
    }

  @Override
  protected Collection<Tuple> createTupleCollection( Tuple tuple )
    {
    Collection<Tuple> collection = tupleCollectionFactory.create( flowProcess );

    if( collection instanceof Spillable )
      {
      ( (Spillable) collection ).setGrouping( tuple );
      ( (Spillable) collection ).setSpillListener( getSpillListener() );
      ( (Spillable) collection ).setSpillStrategy( spillStrategy );
      }

    return collection;
    }

  /**
   * Method getSpillStrategy returns a SpillStrategy instance that is passed to the underlying Spillable
   * tuple collection.
   *
   * @return of type Spillable#SpillStrategy
   */
  protected Spillable.SpillStrategy getSpillStrategy()
    {
    return new Spillable.SpillStrategy()
    {
    int minThreshold = (int) ( getMapThreshold() * .05 );

    int current()
      {
      return Math.max( minThreshold, Math.min( getInitListThreshold(), getMapThreshold() / size() ) );
      }

    @Override
    public boolean doSpill( Spillable spillable, int size )
      {
      return current() <= size;
      }

    @Override
    public String getSpillReason( Spillable spillable )
      {
      return "met current threshold: " + current();
      }
    };
    }
  }
