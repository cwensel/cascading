/*
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

package cascading.stats.tez;

import java.io.IOException;
import java.util.Iterator;

import cascading.flow.FlowNode;
import cascading.flow.FlowStep;
import cascading.management.state.ClientState;
import cascading.stats.BaseCachedStepStats;
import cascading.stats.CascadingStats;
import cascading.stats.FlowNodeStats;
import cascading.stats.tez.util.TezStatsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;

/**
 *
 */
public abstract class TezStepStats extends BaseCachedStepStats<Configuration, DAGClient, TezCounters>
  {
  /**
   * Constructor CascadingStats creates a new CascadingStats instance.
   *
   * @param flowStep
   * @param clientState
   */
  protected TezStepStats( FlowStep flowStep, ClientState clientState )
    {
    super( flowStep, clientState );

    Configuration config = (Configuration) flowStep.getConfig();

    this.counterCache = new TezCounterCache<DAGClient>( this, config )
    {
    @Override
    protected DAGClient getJobStatusClient()
      {
      return TezStepStats.this.getJobStatusClient();
      }

    protected TezCounters getCounters( DAGClient statusClient ) throws IOException
      {
      DAGStatus dagStatus = TezStatsUtil.getDagStatusWithCounters( statusClient );

      if( dagStatus != null )
        {
        TezCounters counters = dagStatus.getDAGCounters();

        if( counters != null && counters.countCounters() != 0 )
          return counters;
        }

      return null;
      }
    };

    Iterator<FlowNode> iterator = flowStep.getFlowNodeGraph().getOrderedTopologicalIterator();

    while( iterator.hasNext() )
      addNodeStats( new TezNodeStats( this, iterator.next(), clientState, config ) );
    }

  /** Method captureDetail captures statistics task details and completion events. */
  @Override
  public void captureDetail( CascadingStats.Type depth )
    {
    if( !getType().isChild( depth ) || !isDetailStale() )
      return;

    DAGClient dagClient = getJobStatusClient();

    if( dagClient == null )
      return;

    synchronized( this )
      {
      if( !isDetailStale() )
        return;

      for( FlowNodeStats flowNodeStats : getFlowNodeStatsMap().values() )
        flowNodeStats.captureDetail( depth );

      markDetailCaptured();
      }
    }
  }
