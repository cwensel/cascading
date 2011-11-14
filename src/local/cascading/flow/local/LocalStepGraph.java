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

package cascading.flow.local;

import java.util.LinkedHashMap;
import java.util.Map;

import cascading.flow.planner.ElementGraph;
import cascading.flow.planner.FlowStep;
import cascading.flow.planner.StepGraph;
import cascading.tap.Tap;
import org.jgrapht.Graphs;

/**
 *
 */
public class LocalStepGraph extends StepGraph
  {
  public LocalStepGraph( String flowName, ElementGraph elementGraph, Map<String, Tap> traps )
    {
    super( flowName, elementGraph, traps );
    }

  @Override
  protected FlowStep createFlowStep( String stepName, int stepNum )
    {
    return new LocalFlowStep( stepName, stepNum );
    }

  protected void makeStepGraph( String flowName, ElementGraph elementGraph, Map<String, Tap> traps )
    {
    Map<String, FlowStep> steps = new LinkedHashMap<String, FlowStep>();
    LocalFlowStep step = (LocalFlowStep) getCreateFlowStep( steps, "local", 1 );

    addVertex( step );

    step.getSourceMap().putAll( elementGraph.getSourceMap() );
    step.getSinkMap().putAll( elementGraph.getSinkMap() );
    step.getTrapMap().putAll( traps );

    Graphs.addGraph( step.getGraph(), elementGraph );

    // remove the extents
    step.getGraph().removeVertex( ElementGraph.head );
    step.getGraph().removeVertex( ElementGraph.tail );

    step.getGroups().addAll( elementGraph.findAllGroups() );
    }
  }
