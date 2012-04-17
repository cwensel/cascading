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

package cascading.flow.hadoop.stream;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.flow.stream.MemoryHashJoinGate;
import cascading.pipe.HashJoin;
import cascading.provider.FactoryLoader;
import cascading.tuple.Tuple;
import cascading.tuple.collect.Spillable;
import cascading.tuple.collect.TupleMapFactory;
import cascading.tuple.hadoop.collect.HadoopTupleMapFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.tuple.collect.TupleMapFactory.TUPLE_MAP_FACTORY;

/**
 *
 */
public class HadoopMemoryJoinGate extends MemoryHashJoinGate
  {
  private static final Logger LOG = LoggerFactory.getLogger( HadoopMemoryJoinGate.class );

  public enum Spill
    {
      Num_Spills_Written, Num_Spills_Read, Num_Tuples_Spilled
    }

  private class SpillListener implements Spillable.SpillListener
    {
    private final FlowProcess flowProcess;

    public SpillListener( FlowProcess flowProcess )
      {
      this.flowProcess = flowProcess;
      }

    @Override
    public void notifyWriteSpillBegin( Spillable spillable, int spillSize, String spillReason )
      {
      int numFiles = spillable.spillCount();

      if( numFiles % 10 == 0 )
        {
        LOG.info( "spilling grouping: {}, num times: {}, with reason: {}",
          new Object[]{spillable.getGrouping().print(), numFiles + 1, spillReason} );

        Runtime runtime = Runtime.getRuntime();
        long freeMem = runtime.freeMemory() / 1024 / 1024;
        long maxMem = runtime.maxMemory() / 1024 / 1024;
        long totalMem = runtime.totalMemory() / 1024 / 1024;

        LOG.info( "mem on spill (mb), free: " + freeMem + ", total: " + totalMem + ", max: " + maxMem );
        }

      LOG.info( "spilling {} tuples in list to file number {}", spillSize, numFiles + 1 );

      flowProcess.increment( Spill.Num_Spills_Written, 1 );
      flowProcess.increment( Spill.Num_Tuples_Spilled, spillSize );
      }

    @Override
    public void notifyReadSpillBegin( Spillable spillable )
      {
      flowProcess.increment( Spill.Num_Spills_Read, 1 );
      }
    }

  private final SpillListener spillListener;
  private TupleMapFactory tupleMapFactory;

  public HadoopMemoryJoinGate( FlowProcess flowProcess, HashJoin join )
    {
    super( flowProcess, join );

    this.spillListener = new SpillListener( flowProcess );

    FactoryLoader loader = FactoryLoader.getInstance();

    this.tupleMapFactory = loader.loadFactoryFrom( flowProcess, TUPLE_MAP_FACTORY, HadoopTupleMapFactory.class );
    }

  @Override
  protected Set<Tuple> createKeySet()
    {
    return new HashSet<Tuple>(); // does not need to be synchronized, or ordered
    }

  @Override
  protected Map<Tuple, Collection<Tuple>> createTupleMap()
    {
    Map<Tuple, Collection<Tuple>> map = tupleMapFactory.create( flowProcess );

    if( map instanceof Spillable )
      ( (Spillable) map ).setSpillListener( spillListener );

    return map;
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
