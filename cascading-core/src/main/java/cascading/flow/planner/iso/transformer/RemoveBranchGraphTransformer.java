/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.Match;
import cascading.util.Util;

/**
 *
 */
public class RemoveBranchGraphTransformer extends MutateGraphTransformer
  {
  public RemoveBranchGraphTransformer( ExpressionGraph filter )
    {
    super( filter );
    }

  public RemoveBranchGraphTransformer( GraphTransformer graphTransformer, ExpressionGraph filter )
    {
    super( graphTransformer, filter );
    }

  @Override
  protected boolean transformGraphInPlaceUsing( Transformed<ElementGraph> transformed, ElementGraph graph, Match match )
    {
    Set<FlowElement> primary = match.getCapturedElements( ElementCapture.Primary );
    Set<FlowElement> secondary = match.getCapturedElements( ElementCapture.Secondary );

    if( primary.isEmpty() )
      return false;

    if( secondary.isEmpty() )
      {
      for( FlowElement flowElement : primary )
        {
        boolean found = ElementGraphs.removeBranchContaining( graph, flowElement );

        if( !found )
          throw new IllegalStateException( "no branch found at: " + flowElement );
        }
      }
    else
      {
      // RemoveStreamedBranchTransformer uses a Secondary match to identify a branch but is currently unused
      if( secondary.size() != 1 )
        throw new IllegalStateException( "too many captured secondary elements" );

      boolean found = ElementGraphs.removeBranchBetween( graph, Util.getFirst( primary ), Util.getFirst( secondary ), false );

      if( !found )
        throw new IllegalStateException( "no branch found between: " + Util.getFirst( primary ) + " and " + Util.getFirst( secondary ) );
      }

    return true;
    }
  }
