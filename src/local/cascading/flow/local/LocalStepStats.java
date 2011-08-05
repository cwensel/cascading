/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow.local;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.flow.planner.FlowStep;
import cascading.management.ClientState;
import cascading.stats.StepStats;

/**
 *
 */
public class LocalStepStats extends StepStats
  {
  final Map<String, Map<String, Long>> counters = new HashMap<String, Map<String, Long>>();

  /** Constructor CascadingStats creates a new CascadingStats instance. */
  protected LocalStepStats( FlowStep flowStep, ClientState clientState )
    {
    super( flowStep, clientState );
    }

  @Override
  public void captureJobStats()
    {
    }

  @Override
  public Collection<String> getCounterGroups()
    {
    return counters.keySet();
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

    if( counterMap == null )
      return 0;

    return counterMap.get( counter.toString() );
    }

  @Override
  public long getCounterValue( String group, String counter )
    {
    Map<String, Long> counterMap = counters.get( group );

    if( counterMap == null )
      return 0;

    return counterMap.get( counter );
    }

  public void increment( Enum counter, int amount )
    {
    increment( counter.getDeclaringClass().getName(), counter.toString(), amount );
    }

  public void increment( String group, String counter, int amount )
    {
    Map<String, Long> groupMap = getCreateCounter( group );

    Long value = groupMap.get( counter );

    if( value == null )
      value = 0L;

    groupMap.put( counter, value + amount );
    }

  private Map<String, Long> getCreateCounter( String group )
    {
    Map<String, Long> counterMap = counters.get( group );

    if( counterMap == null )
      {
      counterMap = new HashMap<String, Long>();
      counters.put( group, counterMap );
      }

    return counterMap;
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
