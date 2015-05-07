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
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementMultiGraph;
import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.Match;
import cascading.util.ProcessLogger;

/**
 *
 */
public class AnnotateGraphTransformer extends RecursiveGraphTransformer<ElementGraph>
  {
  private final ElementAnnotation annotation;

  protected final GraphTransformer graphTransformer;

  public AnnotateGraphTransformer( ExpressionGraph match, ElementAnnotation annotation )
    {
    super( match );
    this.annotation = annotation;
    this.graphTransformer = null;
    }

  public AnnotateGraphTransformer( GraphTransformer graphTransformer, ExpressionGraph match, ElementAnnotation annotation )
    {
    super( match );
    this.annotation = annotation;
    this.graphTransformer = graphTransformer;
    }

  @Override
  protected boolean requiresRecursiveSearch()
    {
    return graphTransformer != null || super.requiresRecursiveSearch();
    }

  @Override
  protected ElementGraph prepareForMatch( ProcessLogger processLogger, Transformed<ElementGraph> transformed, ElementGraph graph )
    {
    if( graphTransformer == null )
      return makeAnnotated( graph );

    Transformed child = graphTransformer.transform( transformed.getPlannerContext(), graph );

    transformed.addChildTransform( child );

    ElementGraph endGraph = child.getEndGraph();

    return makeAnnotated( endGraph );
    }

  private ElementGraph makeAnnotated( ElementGraph endGraph )
    {
    if( endGraph == null )
      return null;

    if( endGraph instanceof AnnotatedGraph )
      return endGraph;

    return new ElementMultiGraph( endGraph );
    }

  @Override
  protected Set<FlowElement> addExclusions( ElementGraph graph )
    {
    return ( (AnnotatedGraph) graph ).getAnnotations().getValues( annotation.getAnnotation() );
    }

  @Override
  protected boolean transformGraphInPlaceUsing( Transformed<ElementGraph> transformed, ElementGraph graph, Match match )
    {
    Set<FlowElement> captured = match.getCapturedElements( annotation.getCapture() );

    if( captured.isEmpty() )
      return false;

    ( (AnnotatedGraph) graph ).getAnnotations().addAll( annotation.getAnnotation(), captured );

    return true;
    }

  }
