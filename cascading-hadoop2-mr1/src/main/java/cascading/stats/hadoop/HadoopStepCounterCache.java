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

package cascading.stats.hadoop;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import cascading.stats.CascadingStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

/**
 *
 */
public abstract class HadoopStepCounterCache extends CounterCache<RunningJob, Counters>
  {
  protected HadoopStepCounterCache( CascadingStats stats, Configuration configuration )
    {
    super( stats, configuration );
    }

  @Override
  protected boolean areCountersAvailable( RunningJob runningJob )
    {
    return true;
    }

  @Override
  protected Counters getCounters( RunningJob runningJob ) throws IOException
    {
    return HadoopStepStats.getJob( runningJob ).getCounters();
    }

  @Override
  protected Collection<String> getGroupNames( Counters groups )
    {
    LinkedHashSet<String> result = new LinkedHashSet<>();

    for( String value : groups.getGroupNames() )
      result.add( value );

    return result;
    }

  @Override
  protected Set<String> getCountersFor( Counters counters, String group )
    {
    Set<String> results = new HashSet<>();

    for( Counter counter : counters.getGroup( group ) )
      results.add( counter.getName() );

    return results;
    }

  @Override
  protected long getCounterValue( Counters counters, Enum counter )
    {
    Counter result = counters.findCounter( counter );

    if( result == null )
      return 0;

    return result.getValue();
    }

  @Override
  protected long getCounterValue( Counters counters, String groupName, String counterName )
    {
    CounterGroup counterGroup = counters.getGroup( groupName );

    if( counterGroup == null )
      return 0;

    // getCounter actually searches the display name, wtf
    // in theory this is lazily created if does not exist, but don't rely on it
    Counter counterValue = counterGroup.findCounter( counterName );

    if( counterValue == null )
      return 0;

    return counterValue.getValue();
    }
  }
