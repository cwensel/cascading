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

import cascading.flow.planner.Scope;

/**
 * If ScopeExpression is the only edge, the {@link ScopeExpression.Applies} enum will apply,
 * otherwise the number of edges on the target graph must match the number of edges in the expression.
 */
public abstract class ScopeExpression implements Expression<Scope>
  {
  public static final PathScopeExpression ANY = new PathScopeExpression( Applies.Any );
  public static final PathScopeExpression ALL = new PathScopeExpression( Applies.All );

  public enum Applies
    {
      Any, All
    }

  protected Applies applies = Applies.Any;

  protected ScopeExpression()
    {
    }

  protected ScopeExpression( Applies applies )
    {
    this.applies = applies;
    }

  public boolean appliesToAllPaths()
    {
    return applies == Applies.All;
    }

  public boolean appliesToAnyPaths()
    {
    return applies == Applies.Any;
    }

  public boolean acceptsAll()
    {
    return appliesToAllPaths();
    }

  public Applies getApplies()
    {
    return applies;
    }
  }
