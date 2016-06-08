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

package cascading.stats.process;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.flow.process.ProcessFlowStep;
import cascading.management.state.ClientState;
import cascading.stats.FlowStepStats;

/**
 * ProcessStepStats is an implementation of FlowStepStats used in non-hadoop based flows like ProcessFlow.
 */
public class ProcessStepStats extends FlowStepStats
  {
  /**
   * The counters of the step as a Map.
   */
  private final Map<String, Map<String, Long>> counters;
  private final long lastFetch;

  /**
   * Constructs a new ProcessStepStats instance.
   *
   * @param clientState
   * @param counters
   * @param step
   */
  public ProcessStepStats( ClientState clientState, Map<String, Map<String, Long>> counters, ProcessFlowStep step )
    {
    super( step, clientState );
    this.counters = counters;

    lastFetch = this.counters != null ? System.currentTimeMillis() : -1;
    }

  @Override
  public void recordChildStats()
    {

    }

  @Override
  public String getProcessStepID()
    {
    return null;
    }

  @Override
  public long getLastSuccessfulCounterFetchTime()
    {
    return lastFetch;
    }

  @Override
  public Collection<String> getCounterGroups()
    {
    return counters.keySet();
    }

  @Override
  public Collection<String> getCountersFor( String group )
    {
    Map<String, Long> groupCollection = counters.get( group );

    if( groupCollection == null )
      return Collections.emptySet();

    return groupCollection.keySet();
    }

  @Override
  public long getCounterValue( Enum counter )
    {
    Map<String, Long> counterMap = counters.get( counter.getDeclaringClass().getName() );

    String counterString = counter.toString();

    if( counterMap == null || !counterMap.containsKey( counterString ) )
      return 0;

    return counterMap.get( counterString );
    }

  @Override
  public long getCounterValue( String group, String counter )
    {
    Map<String, Long> counterMap = counters.get( group );

    if( counterMap == null || !counterMap.containsKey( counter ) )
      return 0;

    return counterMap.get( counter );
    }

  @Override
  public void captureDetail()
    {

    }

  @Override
  public void captureDetail( Type depth )
    {

    }

  @Override
  public Collection<String> getCounterGroupsMatching( String regex )
    {
    Collection<String> counters = getCounterGroups();

    Set<String> results = new HashSet<String>();

    for( String counter : counters )
      {
      if( counter.matches( regex ) )
        results.add( counter );
      }

    return Collections.unmodifiableCollection( results );
    }

  @Override
  public Collection getChildren()
    {
    return Collections.emptyList();
    }
  }
