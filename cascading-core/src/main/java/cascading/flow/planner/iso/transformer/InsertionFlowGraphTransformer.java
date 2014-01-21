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

import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.FlowElementGraph;
import cascading.flow.planner.iso.expression.ElementExpression;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.Match;

/**
 *
 */
public class InsertionFlowGraphTransformer extends MutateFlowGraphTransformer
  {
  private final String factoryName;

  public InsertionFlowGraphTransformer( ExpressionGraph expressionGraph, String factoryName )
    {
    super( expressionGraph );
    this.factoryName = factoryName;

    if( factoryName == null )
      throw new IllegalArgumentException( "factoryName may not be null" );
    }

  public InsertionFlowGraphTransformer( GraphTransformer graphTransformer, ExpressionGraph filter, String factoryName )
    {
    super( graphTransformer, filter );
    this.factoryName = factoryName;

    if( factoryName == null )
      throw new IllegalArgumentException( "factoryName may not be null" );
    }

  @Override
  protected boolean transformGraphInPlaceUsing( Transform<FlowElementGraph> transform, FlowElementGraph graph, Match match )
    {
    Set<FlowElement> insertions = match.getCapturedElements( ElementExpression.Capture.Primary );

    if( insertions.isEmpty() )
      return false;

    ElementFactory elementFactory = transform.getPlannerContext().getElementFactoryFor( factoryName );

    for( FlowElement flowElement : insertions )
      graph.insertFlowElementAfter( flowElement, elementFactory.create( graph, flowElement ) );

    return true;
    }
  }
