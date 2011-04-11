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
    LocalFlowStep step = (LocalFlowStep) getCreateFlowStep( flowName, steps, "local", 1 );

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
