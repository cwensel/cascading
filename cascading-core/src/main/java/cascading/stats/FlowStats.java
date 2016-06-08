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

package cascading.stats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.management.state.ClientState;
import cascading.property.AppProps;
import cascading.util.ProcessLogger;

/** Class FlowStats collects {@link cascading.flow.Flow} specific statistics. */
public class FlowStats extends CascadingStats<FlowStepStats>
  {
  final Flow flow;
  final Map<String, FlowStepStats> flowStepStatsMap = new LinkedHashMap<>(); // retains topological order

  public FlowStats( Flow flow, ClientState clientState )
    {
    super( flow.getName(), clientState );
    this.flow = flow;
    }

  @Override
  protected ProcessLogger getProcessLogger()
    {
    if( flow != null && flow instanceof ProcessLogger )
      return (ProcessLogger) flow;

    return ProcessLogger.NULL;
    }

  public Map<Object, Object> getFlowProperties()
    {
    return flow.getConfigAsProperties();
    }

  public String getAppID()
    {
    return AppProps.getApplicationID( getFlowProperties() );
    }

  public String getAppName()
    {
    return AppProps.getApplicationName( getFlowProperties() );
    }

  @Override
  public String getID()
    {
    return flow.getID();
    }

  @Override
  public Type getType()
    {
    return Type.FLOW;
    }

  public Flow getFlow()
    {
    return flow;
    }

  @Override
  public synchronized void recordInfo()
    {
    clientState.recordFlow( flow );
    }

  public void addStepStats( FlowStepStats flowStepStats )
    {
    flowStepStatsMap.put( flowStepStats.getID(), flowStepStats );
    }

  /**
   * Method getStepStats returns the stepStats owned by this FlowStats.
   *
   * @return the stepStats (type List<StepStats>) of this FlowStats object.
   */
  public List<FlowStepStats> getFlowStepStats()
    {
    return new ArrayList<>( flowStepStatsMap.values() );
    }

  /**
   * Method getStepsCount returns the number of steps this Flow executed.
   *
   * @return the stepsCount (type int) of this FlowStats object.
   */
  public int getStepsCount()
    {
    return flowStepStatsMap.size();
    }

  @Override
  public long getLastSuccessfulCounterFetchTime()
    {
    long max = -1;

    for( FlowStepStats flowStepStats : flowStepStatsMap.values() )
      max = Math.max( max, flowStepStats.getLastSuccessfulCounterFetchTime() );

    return max;
    }

  @Override
  public Collection<String> getCounterGroups()
    {
    Set<String> results = new HashSet<String>();

    for( FlowStepStats flowStepStats : flowStepStatsMap.values() )
      results.addAll( flowStepStats.getCounterGroups() );

    return results;
    }

  @Override
  public Collection<String> getCounterGroupsMatching( String regex )
    {
    Set<String> results = new HashSet<String>();

    for( FlowStepStats flowStepStats : flowStepStatsMap.values() )
      results.addAll( flowStepStats.getCounterGroupsMatching( regex ) );

    return results;
    }

  @Override
  public Collection<String> getCountersFor( String group )
    {
    Set<String> results = new HashSet<String>();

    for( FlowStepStats flowStepStats : flowStepStatsMap.values() )
      results.addAll( flowStepStats.getCountersFor( group ) );

    return results;
    }

  @Override
  public long getCounterValue( Enum counter )
    {
    long value = 0;

    for( FlowStepStats flowStepStats : flowStepStatsMap.values() )
      value += flowStepStats.getCounterValue( counter );

    return value;
    }

  @Override
  public long getCounterValue( String group, String counter )
    {
    long value = 0;

    for( FlowStepStats flowStepStats : flowStepStatsMap.values() )
      value += flowStepStats.getCounterValue( group, counter );

    return value;
    }

  @Override
  public void captureDetail( Type depth )
    {
    if( !getType().isChild( depth ) )
      return;

    for( FlowStepStats flowStepStats : flowStepStatsMap.values() )
      flowStepStats.captureDetail( depth );
    }

  @Override
  public Collection<FlowStepStats> getChildren()
    {
    return flowStepStatsMap.values();
    }

  @Override
  public FlowStepStats getChildWith( String id )
    {
    return flowStepStatsMap.get( id );
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

  @Override
  public int hashCode()
    {
    return getID().hashCode();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || !( object instanceof FlowStats ) )
      return false;

    return getID().equals( ( (FlowStats) object ).getID() );
    }
  }
