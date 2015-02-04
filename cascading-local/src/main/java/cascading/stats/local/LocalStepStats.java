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

package cascading.stats.local;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.FlowStep;
import cascading.management.state.ClientState;
import cascading.stats.FlowStepStats;

import static java.util.Collections.synchronizedMap;
import static java.util.Collections.unmodifiableCollection;

/**
 *
 */
public class LocalStepStats extends FlowStepStats
  {
  final Map<String, Map<String, Long>> counters = synchronizedMap( new HashMap<String, Map<String, Long>>() );

  /** Constructor CascadingStats creates a new CascadingStats instance. */
  public LocalStepStats( FlowStep<Properties> flowStep, ClientState clientState )
    {
    super( flowStep, clientState );
    }

  @Override
  public void recordChildStats()
    {
    }

  @Override
  public Collection<String> getCounterGroups()
    {
    synchronized( counters )
      {
      return unmodifiableCollection( new LinkedHashSet<String>( counters.keySet() ) );
      }
    }

  @Override
  public Collection<String> getCounterGroupsMatching( String regex )
    {
    Collection<String> counterGroups = getCounterGroups();

    Set<String> results = new HashSet<String>();

    synchronized( counters )
      {
      for( String group : counterGroups )
        {
        if( group.matches( regex ) )
          results.add( group );
        }
      }

    return unmodifiableCollection( results );
    }

  @Override
  public Collection<String> getCountersFor( String group )
    {
    Map<String, Long> groupCollection = counters.get( group );

    if( groupCollection == null )
      return Collections.emptySet();

    synchronized( groupCollection )
      {
      return unmodifiableCollection( new LinkedHashSet<String>( groupCollection.keySet() ) );
      }
    }

  @Override
  public long getCounterValue( Enum counter )
    {
    Map<String, Long> counterMap = counters.get( counter.getDeclaringClass().getName() );

    String counterString = counter.toString();

    // we don't remove counters, so safe
    if( counterMap == null || !counterMap.containsKey( counterString ) )
      return 0;

    return counterMap.get( counterString );
    }

  @Override
  public long getCounterValue( String group, String counter )
    {
    Map<String, Long> counterMap = counters.get( group );

    // we don't remove counters, so safe
    if( counterMap == null || !counterMap.containsKey( counter ) )
      return 0;

    return counterMap.get( counter );
    }

  public void increment( Enum counter, long amount )
    {
    increment( counter.getDeclaringClass().getName(), counter.toString(), amount );
    }

  public void increment( String group, String counter, long amount )
    {
    Map<String, Long> groupMap = getCreateCounter( group );

    synchronized( groupMap )
      {
      Long value = groupMap.get( counter );

      if( value == null )
        value = 0L;

      groupMap.put( counter, value + amount );
      }
    }

  private Map<String, Long> getCreateCounter( String group )
    {
    synchronized( counters )
      {
      Map<String, Long> counterMap = counters.get( group );

      if( counterMap == null )
        {
        counterMap = synchronizedMap( new HashMap<String, Long>() );
        counters.put( group, counterMap );
        }

      return counterMap;
      }
    }

  @Override
  public void captureDetail()
    {
    }

  @Override
  public Collection getChildren()
    {
    return Collections.emptyList();
    }
  }
