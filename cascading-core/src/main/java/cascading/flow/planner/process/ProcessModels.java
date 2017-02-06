/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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
import cascading.flow.planner.graph.ElementGraphs;
import cascading.util.Util;

/**
 *
 */
public class ProcessModels
  {
  /**
   * Does the given process perform any work?
   *
   * @param process
   * @param excludingType
   * @return
   */
  public static boolean isIdentity( ProcessModel process, Class<? extends FlowElement> excludingType )
    {
    ElementGraph elementGraph = ElementGraphs.asExtentMaskedSubGraph( process.getElementGraph() );

    Set<? extends FlowElement> sourceElements = Util.narrowIdentitySet( excludingType, process.getSourceElements(), false );
    Set<? extends FlowElement> sinkElements = Util.narrowIdentitySet( excludingType, process.getSinkElements(), false );

    return elementGraph.vertexSet().size() == ( sourceElements.size() + sinkElements.size() );
    }

  /**
   * Does the given process perform any work?
   *
   * @param process
   * @return
   */
  public static boolean isIdentity( ProcessModel process )
    {
    ElementGraph elementGraph = ElementGraphs.asExtentMaskedSubGraph( process.getElementGraph() );

    Set<FlowElement> sourceElements = process.getSourceElements();
    Set<FlowElement> sinkElements = process.getSinkElements();

    return elementGraph.vertexSet().size() == ( sourceElements.size() + sinkElements.size() );
    }
  }
