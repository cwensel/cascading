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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowStep;
import cascading.management.state.ClientState;
import cascading.util.ProcessLogger;

/** Class FlowStepStats collects {@link cascading.flow.FlowStep} specific statistics. */
public abstract class FlowStepStats extends CascadingStats<FlowNodeStats>
  {
  private final FlowStep flowStep;
  private Map<String, FlowNodeStats> flowNodeStatsMap = new LinkedHashMap<>(); // topologically ordered

  protected FlowStepStats( FlowStep flowStep, ClientState clientState )
    {
    super( flowStep.getName(), clientState );
    this.flowStep = flowStep;
    }

  @Override
  protected ProcessLogger getProcessLogger()
    {
    if( flowStep != null && flowStep instanceof ProcessLogger )
      return (ProcessLogger) flowStep;

    return ProcessLogger.NULL;
    }

  @Override
  public String getID()
    {
    return flowStep.getID();
    }

  @Override
  public Type getType()
    {
    return Type.STEP;
    }

  public FlowStep getFlowStep()
    {
    return flowStep;
    }

  public void addNodeStats( FlowNodeStats flowNodeStats )
    {
    flowNodeStatsMap.put( flowNodeStats.getID(), flowNodeStats );
    }

  protected Map<String, FlowNodeStats> getFlowNodeStatsMap()
    {
    return flowNodeStatsMap;
    }

  public List<FlowNodeStats> getFlowNodeStats()
    {
    return new ArrayList<>( flowNodeStatsMap.values() );
    }

  public int getNodesCount()
    {
    return flowNodeStatsMap.size();
    }

  protected Collection<String> getFlowNodeIDs()
    {
    return flowNodeStatsMap.keySet();
    }

  @Override
  public Collection<FlowNodeStats> getChildren()
    {
    return flowNodeStatsMap.values();
    }

  @Override
  public synchronized void recordInfo()
    {
    clientState.recordFlowStep( flowStep );
    }

  @Override
  public String toString()
    {
    return "Step{" + getStatsString() + '}';
    }

  /**
   * An internal method that will refresh current counters and, if a clientState client is enabled, the child details
   * including node and slice statistics.
   * <p/>
   * All results will be then recorded by the clientState implementation.
   * <p/>
   * See {@link #captureDetail()} to force all statistics to be cached and locally accessible.
   */
  public abstract void recordChildStats();

  /**
   * Method getProcessStepID returns the ID representing the under platform process.
   * <p/>
   * A FlowStep represents a unit of work on a remote platform, in the case of MapReduce, a
   * MapReduce job. The step ID would be the job id.
   *
   * @return a String or null if unavailable
   */
  public abstract String getProcessStepID();

  public String getProcessStatusURL()
    {
    return null;
    }
  }
