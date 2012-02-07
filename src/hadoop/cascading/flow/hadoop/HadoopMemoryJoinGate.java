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
import cascading.flow.FlowProcessWrapper;
import cascading.flow.stream.MemoryJoinGate;
import cascading.pipe.Join;
import cascading.tuple.Spillable;
import cascading.tuple.SpillableTupleList;
import cascading.tuple.SpillableTupleMap;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.HadoopSpillableTupleMap;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.tuple.hadoop.HadoopSpillableTupleList.defaultCodecs;
import static cascading.tuple.hadoop.HadoopSpillableTupleList.getCodec;

/**
 *
 */
public class HadoopMemoryJoinGate extends MemoryJoinGate
  {
  private static final Logger LOG = LoggerFactory.getLogger( HadoopMemoryJoinGate.class );

  public enum Spill
    {
      Num_Spills_Written, Num_Spills_Read, Num_Tuples_Spilled
    }

  private class SpillListener implements Spillable.SpillListener
    {
    private final FlowProcess flowProcess;
    private final Tuple tuple;

    public SpillListener( FlowProcess flowProcess, Tuple tuple )
      {
      this.flowProcess = flowProcess;
      this.tuple = tuple;
      }

    @Override
    public void notifySpill( Spillable spillable, Collection current )
      {
      int numFiles = ( (SpillableTupleList) spillable ).getNumFiles();

      if( ( numFiles - 1 ) % 10 == 0 )
        {
        LOG.info( "spilled grouping: {}, num times: {}",
          new Object[]{tuple.print(), numFiles} );

        Runtime runtime = Runtime.getRuntime();
        long freeMem = runtime.freeMemory() / 1024 / 1024;
        long maxMem = runtime.maxMemory() / 1024 / 1024;
        long totalMem = runtime.totalMemory() / 1024 / 1024;

        LOG.info( "mem on spill (mb), free: " + freeMem + ", total: " + totalMem + ", max: " + maxMem );
        }

      LOG.info( "spilling {} tuples in list to file number {}", current.size(), numFiles + 1 );

      flowProcess.increment( Spill.Num_Spills_Written, 1 );
      flowProcess.increment( Spill.Num_Tuples_Spilled, current.size() );
      }

    @Override
    public void notifyRead( Spillable spillable )
      {
      flowProcess.increment( Spill.Num_Spills_Read, 1 );
      }
    }

  private final int capacity;
  private final float loadFactor;
  private final int mapThreshold;
  private final int listThreshold;
  private JobConf jobConf;
  private CompressionCodec codec;

  public HadoopMemoryJoinGate( FlowProcess flowProcess, Join join )
    {
    super( flowProcess, join );

    capacity = SpillableTupleMap.getCapacity( flowProcess, SpillableTupleMap.initialCapacity );
    loadFactor = SpillableTupleMap.getLoadFactor( flowProcess, SpillableTupleMap.loadFactor );
    mapThreshold = SpillableTupleMap.getThreshold( flowProcess, SpillableTupleMap.defaultThreshold );
    listThreshold = SpillableTupleList.getThreshold( flowProcess, SpillableTupleList.defaultThreshold );
    jobConf = ( (HadoopFlowProcess) FlowProcessWrapper.undelegate( flowProcess ) ).getJobConf();
    codec = getCodec( flowProcess, defaultCodecs );
    }

  @Override
  protected Set<Tuple> createKeySet()
    {
    return new HashSet<Tuple>(); // does not need to be synchronized, or ordered
    }

  @Override
  protected Map<Tuple, Collection<Tuple>> createTupleMap()
    {
    return new HadoopSpillableTupleMap( capacity, loadFactor, mapThreshold, listThreshold, codec, jobConf )
    {
    @Override
    public Collection<Tuple> get( final Object object )
      {
      return super.get( getDelegatedTuple( object ) );
      }

    @Override
    protected Spillable.SpillListener getSpillableListener( Tuple tuple )
      {
      return new SpillListener( flowProcess, tuple );
      }
    };
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
