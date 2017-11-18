/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.planner.iso.finder;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementGraphs;
import org.jgrapht.Graph;

/**
 *
 */
class IndexedElementGraph extends IndexedGraph<Graph<FlowElement, Scope>, FlowElement, Scope>
  {
  private final ElementGraph elementGraph;

  public IndexedElementGraph( SearchOrder searchOrder, ElementGraph elementGraph )
    {
    super( searchOrder, ElementGraphs.directed( elementGraph ) );
    this.elementGraph = elementGraph;
    }

  public ElementGraph getElementGraph()
    {
    return elementGraph;
    }
  }
