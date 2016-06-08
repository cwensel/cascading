/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner.iso.expression;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.ElementGraph;

/**
 *
 */
public class AnnotationExpression extends ElementExpression
  {
  Enum annotation = null;

  public AnnotationExpression( Enum annotation )
    {
    this.annotation = annotation;
    }

  public AnnotationExpression( ElementCapture capture, Enum annotation )
    {
    super( capture );
    this.annotation = annotation;
    }

  @Override
  public boolean applies( PlannerContext plannerContext, ElementGraph elementGraph, FlowElement flowElement )
    {
    if( elementGraph instanceof AnnotatedGraph && ( (AnnotatedGraph) elementGraph ).hasAnnotations() )
      return ( (AnnotatedGraph) elementGraph ).getAnnotations().getValues( annotation ).contains( flowElement );

    return false;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "AnnotationExpression{" );
    sb.append( "annotation=" ).append( annotation );
    sb.append( '}' );
    return sb.toString();
    }
  }
