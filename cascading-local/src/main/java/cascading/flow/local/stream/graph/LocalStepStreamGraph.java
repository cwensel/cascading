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

package cascading.flow.local.stream.graph;

import java.util.Properties;

import cascading.flow.FlowElement;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowProcess;
import cascading.flow.local.LocalFlowStep;
import cascading.flow.local.stream.duct.ParallelFork;
import cascading.flow.local.stream.element.LocalGroupByGate;
import cascading.flow.local.stream.element.SyncMergeStage;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Gate;
import cascading.flow.stream.element.MemoryCoGroupGate;
import cascading.flow.stream.element.SinkStage;
import cascading.flow.stream.element.SourceStage;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.NodeStreamGraph;
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.property.PropertyUtil;
import cascading.tap.Tap;

/**
 *
 */
public class LocalStepStreamGraph extends NodeStreamGraph
  {
  private LocalFlowStep step;

  public LocalStepStreamGraph( FlowProcess<Properties> flowProcess, LocalFlowStep step, FlowNode node )
    {
    super( flowProcess, node );
    this.step = step;

    buildGraph();
    setTraps();
    setScopes();

    printGraph( node.getID(), "local", 0 );

    bind();
    }

  protected void buildGraph()
    {
    for( Object rhsElement : node.getSourceTaps() )
      {
      Duct rhsDuct = new SourceStage( tapFlowProcess( (Tap) rhsElement ), (Tap) rhsElement );

      addHead( rhsDuct );

      handleDuct( (FlowElement) rhsElement, rhsDuct );
      }
    }

  @Override
  protected Duct createFork(Duct[] allNext)
    {
    return new ParallelFork( allNext );
    }

  protected Gate createCoGroupGate(CoGroup element, IORole role )
    {
    return new MemoryCoGroupGate( flowProcess, element );
    }

  protected Gate createGroupByGate( GroupBy element, IORole source )
    {
    return new LocalGroupByGate( flowProcess, element );
    }

  @Override
  protected Duct createMergeStage( Merge merge, IORole both )
    {
    return new SyncMergeStage( flowProcess, merge );
    }

  @Override
  protected SinkStage createSinkStage( Tap element )
    {
    return new SinkStage( tapFlowProcess( element ), element );
    }

  private LocalFlowProcess tapFlowProcess( Tap tap )
    {
    Properties defaultProperties = ( (LocalFlowProcess) flowProcess ).getConfig();
    Properties tapProperties = step.getPropertiesMap().get( tap );

    tapProperties = PropertyUtil.createProperties( tapProperties, defaultProperties );

    return new LocalFlowProcess( (LocalFlowProcess) flowProcess, tapProperties );
    }

  }
