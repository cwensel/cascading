/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.flow.hadoop.util.HadoopUtil;
import cascading.stats.CascadingStats;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowSliceStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskReport;

/**
 *
 */
public class HadoopNodeCounterCache extends HadoopCounterCache<FlowNodeStats, Map<String, Map<String, Long>>>
  {
  private FlowNodeStats flowNodeStats;

  private Configuration configuration;

  protected HadoopNodeCounterCache( FlowNodeStats flowNodeStats, Configuration configuration )
    {
    super( flowNodeStats, configuration );

    this.flowNodeStats = flowNodeStats;
    this.configuration = configuration;
    }

  @Override
  protected FlowNodeStats getJobStatusClient()
    {
    return flowNodeStats;
    }

  @Override
  protected boolean areCountersAvailable( FlowNodeStats runningJob )
    {
    return !HadoopUtil.isLocal( this.configuration );
    }

  protected Map<String, Map<String, Long>> getCounters( FlowNodeStats flowNodeStats ) throws IOException
    {
    // will use final or cached remote stats
    flowNodeStats.captureDetail( CascadingStats.Type.SLICE );

    Map<String, Map<String, Long>> allCounters = new HashMap<>();

    Collection<FlowSliceStats> children = flowNodeStats.getChildren();

    for( FlowSliceStats sliceStats : children )
      {
      TaskReport taskReport = ( (HadoopSliceStats) sliceStats ).getTaskReport();

      Counters counters = taskReport.getTaskCounters();

      for( CounterGroup group : counters )
        {
        Map<String, Long> values = allCounters.get( group.getName() );

        if( values == null )
          {
          values = new HashMap<>();
          allCounters.put( group.getName(), values );
          }

        for( Counter counter : group )
          {
          Long value = values.get( counter.getName() );

          if( value == null )
            value = 0L;

          value += counter.getValue();

          values.put( counter.getName(), value );
          }
        }
      }

    return allCounters;
    }

  protected Collection<String> getGroupNames( Map<String, Map<String, Long>> groups )
    {
    return groups.keySet();
    }

  protected Set<String> getCountersFor( Map<String, Map<String, Long>> counters, String group )
    {
    Set<String> results = new HashSet<>();

    Map<String, Long> map = counters.get( group );

    if( map != null )
      results.addAll( map.keySet() );

    return results;
    }

  protected long getCounterValue( Map<String, Map<String, Long>> counters, Enum counter )
    {
    return getCounterValue( counters, counter.getDeclaringClass().getName(), counter.name() );
    }

  protected long getCounterValue( Map<String, Map<String, Long>> counters, String groupName, String counterName )
    {
    Map<String, Long> counterGroup = counters.get( groupName );

    if( counterGroup == null )
      return 0;

    Long counterValue = counterGroup.get( counterName );

    if( counterValue == null )
      return 0;

    return counterValue;
    }
  }
