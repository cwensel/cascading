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
import java.util.List;
import java.util.Set;

import cascading.flow.FlowNode;
import cascading.management.state.ClientState;
import cascading.stats.hadoop.BaseHadoopNodeStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
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

  static final Set<StatusGetOpts> statusGetOpts = EnumSet.of( StatusGetOpts.GET_COUNTERS );

  private TezStepStats parentStepStats;

  /**
   * Constructor CascadingStats creates a new CascadingStats instance.
   *
   * @param flowNode
   * @param clientState
   * @param configuration
   */
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
        return dagClient.getVertexStatus( getID(), statusGetOpts ).getVertexCounters();
        }
      catch( TezException exception )
        {
        LOG.warn( "unable to get counters", exception );
        return null;
        }
      }

    };
    }

  @Override
  public void captureDetail()
    {
    DAGClient dagClient = parentStepStats.getDAGClient();
    DAGStatus dagStatus = parentStepStats.getJobStatusClient();

    if( dagClient == null || dagStatus == null )
      return;

    try
      {
      VertexStatus vertexStatus = dagClient.getVertexStatus( getID(), statusGetOpts );

      if( vertexStatus == null )
        return;

      List<String> diagnostics = vertexStatus.getDiagnostics();

      for( String diagnostic : diagnostics )
        LOG.info( "{}", diagnostic );

      }
    catch( IOException | TezException exception )
      {
      LOG.warn( "unable to get slice stats", exception );
      }
    }
  }
