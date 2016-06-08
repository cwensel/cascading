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

import cascading.flow.FlowNode;
import cascading.flow.planner.BaseFlowNode;
import cascading.management.state.ClientState;
import cascading.util.ProcessLogger;

/** Class FlowNodeStats collects {@link cascading.flow.FlowNode} specific statistics. */
public abstract class FlowNodeStats extends CascadingStats<FlowSliceStats>
  {
  private final FlowNode flowNode;

  protected boolean hasCapturedFinalDetail = false;

  protected FlowNodeStats( FlowNode flowNode, ClientState clientState )
    {
    super( flowNode.getName(), clientState );
    this.flowNode = flowNode;

    ( (BaseFlowNode) this.flowNode ).setFlowNodeStats( this );
    }

  public abstract String getKind();

  @Override
  protected ProcessLogger getProcessLogger()
    {
    if( flowNode != null && flowNode instanceof ProcessLogger )
      return (ProcessLogger) flowNode;

    return ProcessLogger.NULL;
    }

  @Override
  public String getID()
    {
    return flowNode.getID();
    }

  @Override
  public Type getType()
    {
    return Type.NODE;
    }

  public FlowNode getFlowNode()
    {
    return flowNode;
    }

  public int getOrdinal()
    {
    return flowNode.getOrdinal();
    }

  @Override
  public synchronized void recordInfo()
    {
    clientState.recordFlowNode( flowNode );
    }

  @Override
  public String toString()
    {
    return "Node{" + getStatsString() + '}';
    }

  public abstract void recordChildStats();

  public boolean hasCapturedFinalDetail()
    {
    return hasCapturedFinalDetail;
    }
  }
