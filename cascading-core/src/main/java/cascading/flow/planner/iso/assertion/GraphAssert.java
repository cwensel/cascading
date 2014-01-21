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

package cascading.flow.planner.iso.assertion;

import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.GraphFinder;
import cascading.flow.planner.iso.finder.Match;

/**
 * TODO: add level for warning or error, and fail on errors
 */
public abstract class GraphAssert<E extends ElementGraph>
  {
  private final GraphFinder finder;
  private final String message;

  public GraphAssert( ExpressionGraph expressionGraph, String message )
    {
    this.finder = new GraphFinder( expressionGraph );
    this.message = message;
    }

  protected abstract E prepareForMatch( PlannerContext plannerContext, E graph );

  public Assertion assertion( PlannerContext plannerContext, E graph )
    {
    graph = prepareForMatch( plannerContext, graph );

    Match match = finder.findFirstMatch( plannerContext, graph );

    return new Assertion( message, match );
    }
  }
