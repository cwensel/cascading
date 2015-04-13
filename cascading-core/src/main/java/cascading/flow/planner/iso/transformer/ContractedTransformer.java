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

package cascading.flow.planner.iso.transformer;

import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.graph.ElementMultiGraph;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.Match;

/**
 *
 */
public class ContractedTransformer extends RecursiveGraphTransformer<ElementGraph>
  {
  public ContractedTransformer( ExpressionGraph expression )
    {
    super( expression );
    }

  @Override
  public Transformed<ElementGraph> transform( PlannerContext plannerContext, ElementGraph rootGraph )
    {
    return super.transform( plannerContext, new ElementMultiGraph( rootGraph ) );
    }

  @Override
  protected boolean transformGraphInPlaceUsing( Transformed<ElementGraph> transformed, ElementGraph graph, Match match )
    {
    Set<FlowElement> remove = match.getCapturedElements( ElementCapture.Primary );

    if( remove.isEmpty() )
      return false;

    for( FlowElement flowElement : remove )
      ElementGraphs.removeAndContract( graph, flowElement );

    return true;
    }
  }
