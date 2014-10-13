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

package cascading.stats.tez;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cascading.flow.FlowNode;
import cascading.management.state.ClientState;
import cascading.stats.FlowSliceStats;
import cascading.stats.hadoop.BaseHadoopNodeStats;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TezNodeStats extends BaseHadoopNodeStats<DAGClient, TezCounters>
  {
  private static final Logger LOG = LoggerFactory.getLogger( TezNodeStats.class );

  private static final Set<StatusGetOpts> STATUS_GET_OPTS = EnumSet.of( StatusGetOpts.GET_COUNTERS );

  private TezStepStats parentStepStats;

  private int totalTaskCount;
  private int succeededTaskCount;
  private int failedTaskCount;
  private int killedTaskCount;
  private int runningTaskCount;

  protected TezNodeStats( final TezStepStats parentStepStats, FlowNode flowNode, ClientState clientState, Configuration configuration )
    {
    super( flowNode, clientState );

    this.parentStepStats = parentStepStats;
    this.counterCache = new TezCounterCache<DAGClient>( this, configuration )
    {
    @Override
    protected DAGClient getJobStatusClient()
      {
      return parentStepStats.getDAGClient();
      }

    protected TezCounters getCounters( DAGClient dagClient ) throws IOException
      {
      try
        {
        VertexStatus vertexStatus = dagClient.getVertexStatus( getID(), STATUS_GET_OPTS );

        if( vertexStatus == null )
          {
          LOG.warn( "could not retrieve vertex status for: {}, in stats status: {}", getID(), getStatus() );
          return null;
          }

        TezCounters vertexCounters = vertexStatus.getVertexCounters();

        if( vertexCounters == null )
          LOG.warn( "could not retrieve vertex counters for: {}, in stats status: {}, and vertex state: {}", getID(), getStatus(), vertexStatus.getState() );

        return vertexCounters;
        }
      catch( TezException exception )
        {
        LOG.warn( "unable to get counters", exception );
        return null;
        }
      }
    };
    }

  public int getTotalTaskCount()
    {
    return totalTaskCount;
    }

  public int getSucceededTaskCount()
    {
    return succeededTaskCount;
    }

  public int getFailedTaskCount()
    {
    return failedTaskCount;
    }

  public int getKilledTaskCount()
    {
    return killedTaskCount;
    }

  public int getRunningTaskCount()
    {
    return runningTaskCount;
    }

  @Override
  protected boolean captureDetailInternal()
    {
    DAGClient dagClient = parentStepStats.getDAGClient();
    DAGStatus dagStatus = parentStepStats.getJobStatusClient();

    if( dagClient == null || dagStatus == null )
      return false;

    try
      {
      VertexStatus vertexStatus = dagClient.getVertexStatus( getID(), STATUS_GET_OPTS );

      if( vertexStatus == null )
        return false;

      Progress progress = vertexStatus.getProgress();

      totalTaskCount = progress.getTotalTaskCount();
      runningTaskCount = progress.getRunningTaskCount();
      succeededTaskCount = progress.getSucceededTaskCount();
      failedTaskCount = progress.getFailedTaskCount();
      killedTaskCount = progress.getKilledTaskCount();

      int total = sliceStatsMap.size();

      for( int i = total; i < totalTaskCount; i++ )
        {
        TezSliceStats sliceStats = new TezSliceStats( Util.createUniqueID(), this.getStatus() );
        sliceStatsMap.put( sliceStats.getID(), sliceStats );
        }

      // a placeholder to simulate actual slice stats for now
      Iterator<FlowSliceStats> iterator = sliceStatsMap.values().iterator();

      for( int i = 0; i < runningTaskCount && iterator.hasNext(); i++ )
        ( (TezSliceStats) iterator.next() ).setStatus( Status.RUNNING );

      for( int i = 0; i < succeededTaskCount && iterator.hasNext(); i++ )
        ( (TezSliceStats) iterator.next() ).setStatus( Status.SUCCESSFUL );

      for( int i = 0; i < failedTaskCount && iterator.hasNext(); i++ )
        ( (TezSliceStats) iterator.next() ).setStatus( Status.FAILED );

      for( int i = 0; i < killedTaskCount && iterator.hasNext(); i++ )
        ( (TezSliceStats) iterator.next() ).setStatus( Status.STOPPED );

      List<String> diagnostics = vertexStatus.getDiagnostics();

      for( String diagnostic : diagnostics )
        LOG.info( "vertex diagnostics: {}", diagnostic );

      return true;
      }
    catch( IOException | TezException exception )
      {
      LOG.warn( "unable to get slice stats", exception );
      }

    return false;
    }
  }
