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
import java.util.HashSet;
import java.util.Set;

import cascading.stats.CascadingStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.RunningJob;

/**
 *
 */
public abstract class HadoopStepCounterCache extends HadoopCounterCache<RunningJob, Counters>
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

  protected Counters getCounters( RunningJob runningJob ) throws IOException
    {
    return runningJob.getCounters();
    }

  protected Collection<String> getGroupNames( Counters groups )
    {
    return groups.getGroupNames();
    }

  protected Set<String> getCountersFor( Counters counters, String group )
    {
    Set<String> results = new HashSet<>();

    for( Counters.Counter counter : counters.getGroup( group ) )
      results.add( counter.getName() );

    return results;
    }

  protected long getCounterValue( Counters counters, Enum counter )
    {
    return counters.getCounter( counter );
    }

  protected long getCounterValue( Counters counters, String groupName, String counterName )
    {
    Counters.Group counterGroup = counters.getGroup( groupName );

    if( counterGroup == null )
      return 0;

    // getCounter actually searches the display name, wtf
    // in theory this is lazily created if does not exist, but don't rely on it
    Counters.Counter counterValue = counterGroup.getCounterForName( counterName );

    if( counterValue == null )
      return 0;

    return counterValue.getValue();
    }
  }
