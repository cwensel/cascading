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

package cascading.flow.planner.iso.transformer;

import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.Match;

/**
 * GraphTransformer that uses the supplied factory to generate the replacement FlowElement.
 * <p>
 * Note: This only works if exactly one FlowElement is being replaced.
 */
public class ReplaceGraphFactoryBasedTransformer extends MutateGraphTransformer
  {
  private final String factoryName;

  public ReplaceGraphFactoryBasedTransformer( ExpressionGraph filter, String factoryName )
    {
    super( null, filter );
    this.factoryName = factoryName;

    if( factoryName == null )
      throw new IllegalArgumentException( "factoryName may not be null" );
    }

  public ReplaceGraphFactoryBasedTransformer( GraphTransformer graphTransformer, ExpressionGraph filter, String factoryName )
    {
    super( graphTransformer, filter );
    this.factoryName = factoryName;

    if( factoryName == null )
      throw new IllegalArgumentException( "factoryName may not be null" );
    }

  @Override
  protected boolean transformGraphInPlaceUsing( Transformed<ElementGraph> transformed, ElementGraph graph, Match match )
    {
    ElementFactory elementFactory = transformed.getPlannerContext().getElementFactoryFor( factoryName );

    if( elementFactory == null )
      return false;

    Set<FlowElement> captured = match.getCapturedElements( ElementCapture.Primary );

    if( captured.isEmpty() )
      return false;
    else if( captured.size() != 1 )
      throw new IllegalStateException( "expected one, but found multiple flow elements in the match expression: " + captured );

    FlowElement replace = captured.iterator().next();
    FlowElement replaceWith = elementFactory.create( graph, replace );
    ElementGraphs.replaceElementWith( graph, replace, replaceWith );

    return true;
    }
  }
