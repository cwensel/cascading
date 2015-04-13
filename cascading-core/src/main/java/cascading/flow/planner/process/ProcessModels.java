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

package cascading.flow.planner.process;

import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.graph.ElementGraph;
import cascading.util.Util;

/**
 *
 */
public class ProcessModels
  {
  public static boolean isIdentity( ProcessModel process, Class<? extends FlowElement> excludingType )
    {
    ElementGraph elementGraph = process.getMaskedElementGraph();

    Set<? extends FlowElement> sourceElements = Util.narrowSet( excludingType, process.getSourceElements(), false );
    Set<? extends FlowElement> sinkElements = Util.narrowSet( excludingType, process.getSinkElements(), false );

    return elementGraph.vertexSet().size() == ( sourceElements.size() + sinkElements.size() );
    }

  public static boolean isIdentity( ProcessModel process )
    {
    ElementGraph elementGraph = process.getMaskedElementGraph();
    Set<FlowElement> sourceElements = process.getSourceElements();
    Set<FlowElement> sinkElements = process.getSinkElements();

    return elementGraph.vertexSet().size() == ( sourceElements.size() + sinkElements.size() );
    }
  }
