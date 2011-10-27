/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.stats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.management.ClientState;


/** Class FlowStats collects {@link Flow} specific statistics. */
public class FlowStats extends CascadingStats
  {
  final Flow flow;
  final List<StepStats> stepStatsList = new ArrayList<StepStats>();

  public FlowStats( Flow flow, ClientState clientState )
    {
    super( flow.getName(), clientState );
    this.flow = flow;
    }

  public Map<Object, Object> getFlowProperties()
    {
    return flow.getConfigAsProperties();
    }

  public String getAppID()
    {
    return FlowConnector.getApplicationID( getFlowProperties() );
    }

  public String getAppName()
    {
    return FlowConnector.getApplicationName( getFlowProperties() );
    }

  @Override
  public String getID()
    {
    return flow.getID();
    }

  @Override
  public void recordInfo()
    {
    clientState.recordFlow( flow );
    }

  public void addStepStats( StepStats stepStats )
    {
    stepStatsList.add( stepStats );
    }

  /**
   * Method getStepStats returns the stepStats owned by this FlowStats.
   *
   * @return the stepStats (type List<StepStats>) of this FlowStats object.
   */
  public List<StepStats> getStepStats()
    {
    return stepStatsList;
    }

  /**
   * Method getStepsCount returns the number of steps this Flow executed.
   *
   * @return the stepsCount (type int) of this FlowStats object.
   */
  public int getStepsCount()
    {
    return stepStatsList.size();
    }

  @Override
  public Collection<String> getCounterGroups()
    {
    Set<String> results = new HashSet<String>();

    for( StepStats stepStats : stepStatsList )
      results.addAll( stepStats.getCounterGroups() );

    return results;
    }

  @Override
  public Collection<String> getCounterGroupsMatching( String regex )
    {
    Set<String> results = new HashSet<String>();

    for( StepStats stepStats : stepStatsList )
      results.addAll( stepStats.getCounterGroupsMatching( regex ) );

    return results;
    }

  @Override
  public Collection<String> getCountersFor( String group )
    {
    Set<String> results = new HashSet<String>();

    for( StepStats stepStats : stepStatsList )
      results.addAll( stepStats.getCountersFor( group ) );

    return results;
    }

  @Override
  public long getCounterValue( Enum counter )
    {
    long value = 0;

    for( StepStats stepStats : stepStatsList )
      value += stepStats.getCounterValue( counter );

    return value;
    }

  @Override
  public long getCounterValue( String group, String counter )
    {
    long value = 0;

    for( StepStats stepStats : stepStatsList )
      value += stepStats.getCounterValue( group, counter );

    return value;
    }

  @Override
  public void captureDetail()
    {
    for( StepStats stepStats : stepStatsList )
      stepStats.captureDetail();
    }

  @Override
  public Collection getChildren()
    {
    return getStepStats();
    }

  @Override
  protected String getStatsString()
    {
    return super.getStatsString() + ", stepsCount=" + getStepsCount();
    }

  @Override
  public String toString()
    {
    return "Flow{" + getStatsString() + '}';
    }
  }
