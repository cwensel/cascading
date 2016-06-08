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

package cascading.stats.tez;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import cascading.stats.CascadingStats;
import cascading.stats.CounterCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;

/**
 *
 */
public abstract class TezCounterCache<JobStatus> extends CounterCache<Configuration, JobStatus, TezCounters>
  {
  protected TezCounterCache( CascadingStats stats, Configuration configuration )
    {
    super( stats, configuration );
    }

  @Override
  protected boolean areCountersAvailable( JobStatus runningJob )
    {
    return true;
    }

  @Override
  protected Collection<String> getGroupNames( TezCounters counterGroups )
    {
    Iterable<String> iterable = counterGroups.getGroupNames();

    if( iterable == null ) // it's hadoop, be defensive
      return Collections.emptySet();

    Set<String> groupNames = new HashSet<>();

    for( String groupName : iterable )
      groupNames.add( groupName );

    return groupNames;
    }

  @Override
  protected Set<String> getCountersFor( TezCounters counterGroups, String group )
    {
    Set<String> results = new HashSet<>();

    for( TezCounter counter : counterGroups.getGroup( group ) )
      results.add( counter.getName() );

    return results;
    }

  @Override
  protected long getCounterValue( TezCounters counterGroups, Enum counter )
    {
    TezCounter tezCounter = counterGroups.findCounter( counter );

    if( tezCounter == null )
      return 0;

    return tezCounter.getValue();
    }

  @Override
  protected long getCounterValue( TezCounters counterGroups, String groupName, String counterName )
    {
    CounterGroup counterGroup = counterGroups.getGroup( groupName );

    if( counterGroup == null )
      return 0;

    TezCounter counterValue = counterGroup.findCounter( counterName );

    if( counterValue == null )
      return 0;

    return counterValue.getValue();
    }

  protected int getIntProperty( String property, int defaultValue )
    {
    return configuration.getInt( property, defaultValue );
    }
  }
