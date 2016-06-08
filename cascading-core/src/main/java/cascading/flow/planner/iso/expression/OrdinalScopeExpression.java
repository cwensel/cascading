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

import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;

/**
 *
 */
public class OrdinalScopeExpression extends ScopeExpression
  {
  public static final OrdinalScopeExpression ORDINAL_ZERO = new OrdinalScopeExpression( 0 );
  public static final OrdinalScopeExpression NOT_ORDINAL_ZERO = new OrdinalScopeExpression( true, 0 );

  boolean not = false;
  int ordinal = 0;

  public OrdinalScopeExpression( int ordinal )
    {
    this.ordinal = ordinal;
    }

  public OrdinalScopeExpression( Applies applies, int ordinal )
    {
    super( applies );
    this.ordinal = ordinal;
    }

  public OrdinalScopeExpression( boolean not, int ordinal )
    {
    this.not = not;
    this.ordinal = ordinal;
    }

  public OrdinalScopeExpression( Applies applies, boolean not, int ordinal )
    {
    super( applies );
    this.not = not;
    this.ordinal = ordinal;
    }

  @Override
  public boolean applies( PlannerContext plannerContext, ElementGraph elementGraph, Scope scope )
    {
    boolean equals = scope.getOrdinal().equals( ordinal );

    if( !not )
      return equals;
    else
      return !equals;
    }
  }
