/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner.iso.transformer;

import cascading.flow.planner.FlowElementGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.expression.ExpressionGraph;

/**
 *
 */
public abstract class MutateFlowGraphTransformer extends RecursiveGraphTransformer<FlowElementGraph>
  {
  protected final GraphTransformer graphTransformer;

  public MutateFlowGraphTransformer( ExpressionGraph filter )
    {
    super( filter );
    this.graphTransformer = null;
    }

  public MutateFlowGraphTransformer( GraphTransformer graphTransformer, ExpressionGraph filter )
    {
    super( filter );
    this.graphTransformer = graphTransformer;
    }

  @Override
  protected ElementGraph prepareForMatch( Transform<FlowElementGraph> transform, FlowElementGraph graph )
    {
    if( graphTransformer == null )
      return graph;

    Transform child = graphTransformer.transform( transform.getPlannerContext(), graph );

    transform.addChildTransform( child );

    return child.getEndGraph();
    }
  }