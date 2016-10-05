/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

import java.util.Arrays;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;

/**
 *
 */
public class OrElementExpression extends ElementExpression
  {
  public static ElementExpression or( String name, ElementExpression... elementMatchers )
    {
    return new OrElementExpression( name, elementMatchers );
    }

  public static ElementExpression or( String name, ElementCapture capture, ElementExpression... elementMatchers )
    {
    return new OrElementExpression( name, capture, elementMatchers );
    }

  public static ElementExpression or( ElementExpression... elementMatchers )
    {
    return new OrElementExpression( elementMatchers );
    }

  public static ElementExpression or( ElementCapture capture, ElementExpression... elementMatchers )
    {
    return new OrElementExpression( capture, elementMatchers );
    }

  String name;
  ElementExpression[] matchers;

  public OrElementExpression( String name, ElementExpression... matchers )
    {
    this.name = name;
    this.matchers = matchers;
    }

  public OrElementExpression( String name, ElementCapture capture, ElementExpression... matchers )
    {
    super( capture );
    this.name = name;
    this.matchers = matchers;
    }

  public OrElementExpression( ElementExpression... matchers )
    {
    this.matchers = matchers;
    }

  public OrElementExpression( ElementCapture capture, ElementExpression... matchers )
    {
    super( capture );
    this.matchers = matchers;
    }

  @Override
  public boolean applies( PlannerContext plannerContext, ElementGraph elementGraph, FlowElement flowElement )
    {
    for( ElementExpression matcher : matchers )
      {
      if( matcher.applies( plannerContext, elementGraph, flowElement ) )
        return true;
      }

    return false;
    }

  @Override
  public String toString()
    {
    if( name != null )
      return name;

    final StringBuilder sb = new StringBuilder( "Or{" );
    sb.append( Arrays.toString( matchers ) );
    sb.append( '}' );
    return sb.toString();
    }
  }
