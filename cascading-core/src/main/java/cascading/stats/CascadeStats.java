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

package cascading.stats;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import cascading.cascade.Cascade;
import cascading.management.state.ClientState;

/** Class CascadeStats collects {@link Cascade} specific statistics. */
public class CascadeStats extends CascadingStats<FlowStats>
  {
  private Cascade cascade;
  /** Field flowStatsList */
  final List<FlowStats> flowStatsList = new LinkedList<FlowStats>(); // maintain order

  public CascadeStats( Cascade cascade, ClientState clientState )
    {
    super( cascade.getName(), clientState );
    this.cascade = cascade;
    }

  @Override
  public String getID()
    {
    return cascade.getID();
    }

  @Override
  public Type getType()
    {
    return Type.CASCADE;
    }

  public Cascade getCascade()
    {
    return cascade;
    }

  @Override
  public synchronized void recordInfo()
    {
    clientState.recordCascade( cascade );
    }

  /**
   * Method addFlowStats add a child {@link cascading.flow.Flow} {2link FlowStats} instance.
   *
   * @param flowStats of type FlowStats
   */
  public void addFlowStats( FlowStats flowStats )
    {
    flowStatsList.add( flowStats );
    }

  /**
   * Method getFlowCount returns the number of {@link cascading.flow.Flow}s executed by the Cascade.
   *
   * @return the flowCount (type int) of this CascadeStats object.
   */
  public int getFlowCount()
    {
    return flowStatsList.size();
    }

  @Override
  public Collection<String> getCounterGroups()
    {
    Set<String> results = new HashSet<String>();

    for( FlowStats flowStats : flowStatsList )
      results.addAll( flowStats.getCounterGroups() );

    return results;
    }

  @Override
  public Collection<String> getCounterGroupsMatching( String regex )
    {
    Set<String> results = new HashSet<String>();

    for( FlowStats flowStats : flowStatsList )
      results.addAll( flowStats.getCounterGroupsMatching( regex ) );

    return results;
    }

  @Override
  public Collection<String> getCountersFor( String group )
    {
    Set<String> results = new HashSet<String>();

    for( FlowStats flowStats : flowStatsList )
      results.addAll( flowStats.getCountersFor( group ) );

    return results;
    }

  @Override
  public long getCounterValue( Enum counter )
    {
    long value = 0;

    for( FlowStats flowStats : flowStatsList )
      value += flowStats.getCounterValue( counter );

    return value;
    }

  @Override
  public long getCounterValue( String group, String counter )
    {
    long value = 0;

    for( FlowStats flowStats : flowStatsList )
      value += flowStats.getCounterValue( group, counter );

    return value;
    }

  @Override
  public void captureDetail( Type depth )
    {
    if( !getType().isChild( depth ) )
      return;

    for( FlowStats flowStats : flowStatsList )
      flowStats.captureDetail( depth );
    }

  @Override
  public Collection<FlowStats> getChildren()
    {
    return flowStatsList;
    }

  @Override
  public String toString()
    {
    return "Cascade{" + "flowStatsList=" + flowStatsList + '}';
    }
  }
