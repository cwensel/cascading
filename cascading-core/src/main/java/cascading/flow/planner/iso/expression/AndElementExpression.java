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

package cascading.flow.planner.iso.expression;

import java.util.Arrays;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;

/**
 *
 */
public class AndElementExpression extends ElementExpression
  {
  public static ElementExpression and( ElementExpression... elementMatchers )
    {
    return new AndElementExpression( elementMatchers );
    }

  public static ElementExpression and( ElementCapture capture, ElementExpression... elementMatchers )
    {
    return new AndElementExpression( capture, elementMatchers );
    }

  ElementExpression[] matchers;

  public AndElementExpression( ElementExpression... matchers )
    {
    this.matchers = matchers;
    }

  public AndElementExpression( ElementCapture capture, ElementExpression... matchers )
    {
    super( capture );
    this.matchers = matchers;
    }

  @Override
  public boolean applies( PlannerContext plannerContext, ElementGraph elementGraph, FlowElement flowElement )
    {
    for( ElementExpression matcher : matchers )
      {
      if( !matcher.applies( plannerContext, elementGraph, flowElement ) )
        return false;
      }

    return true;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "And{" );
    sb.append( Arrays.toString( matchers ) );
    sb.append( '}' );
    return sb.toString();
    }
  }
