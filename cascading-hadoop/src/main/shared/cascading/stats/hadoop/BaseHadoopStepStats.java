/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.stats.hadoop;

import java.util.Collection;
import java.util.Map;

import cascading.flow.FlowStep;
import cascading.management.state.ClientState;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStepStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class BaseHadoopStepStats<JobStatusClient, Counters> extends FlowStepStats
  {
  private static final Logger LOG = LoggerFactory.getLogger( BaseHadoopStepStats.class );

  protected HadoopCounterCache<JobStatusClient, Counters> counterCache;

  public BaseHadoopStepStats( FlowStep flowStep, ClientState clientState )
    {
    super( flowStep, clientState );
    }

  /**
   * Method getRunningJob returns the Hadoop {@link org.apache.hadoop.mapred.RunningJob} managing this Hadoop job.
   *
   * @return the runningJob (type RunningJob) of this HadoopStepStats object.
   */
  public abstract JobStatusClient getJobStatusClient();

  @Override
  public long getLastSuccessfulCounterFetchTime()
    {
    if( counterCache != null )
      return counterCache.getLastSuccessfulFetch();

    return -1;
    }

  /**
   * Method getCounterGroups returns all of the Hadoop counter groups.
   *
   * @return the counterGroups (type Collection<String>) of this HadoopStepStats object.
   */
  @Override
  public Collection<String> getCounterGroups()
    {
    return counterCache.getCounterGroups();
    }

  /**
   * Method getCounterGroupsMatching returns all the Hadoop counter groups that match the give regex pattern.
   *
   * @param regex of String
   * @return Collection<String>
   */
  @Override
  public Collection<String> getCounterGroupsMatching( String regex )
    {
    return counterCache.getCounterGroupsMatching( regex );
    }

  /**
   * Method getCountersFor returns the Hadoop counters for the given group.
   *
   * @param group of String
   * @return Collection<String>
   */
  @Override
  public Collection<String> getCountersFor( String group )
    {
    return counterCache.getCountersFor( group );
    }

  /**
   * Method getCounterValue returns the Hadoop counter value for the given counter enum.
   *
   * @param counter of Enum
   * @return long
   */
  @Override
  public long getCounterValue( Enum counter )
    {
    return counterCache.getCounterValue( counter );
    }

  /**
   * Method getCounterValue returns the Hadoop counter value for the given group and counter name.
   *
   * @param group   of String
   * @param counter of String
   * @return long
   */
  @Override
  public long getCounterValue( String group, String counter )
    {
    return counterCache.getCounterValue( group, counter );
    }

  protected synchronized Counters cachedCounters( boolean force )
    {
    return counterCache.cachedCounters( force );
    }

  /** Synchronized to prevent state changes mid record, #stop may be called out of band */
  @Override
  public synchronized void recordChildStats()
    {
    try
      {
      cachedCounters( true );
      }
    catch( Exception exception )
      {
      // do nothing
      }

    if( !clientState.isEnabled() )
      return;

    captureDetail( Type.ATTEMPT );

    try
      {
      for( Map.Entry<String, FlowNodeStats> entry : getFlowNodeStatsMap().entrySet() )
        {
        entry.getValue().recordStats();
        entry.getValue().recordChildStats();
        }
      }
    catch( Exception exception )
      {
      LOG.error( "unable to record node stats", exception );
      }
    }
  }
