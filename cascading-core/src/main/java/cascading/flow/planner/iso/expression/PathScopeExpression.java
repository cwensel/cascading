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

import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;

/**
 * All is a special case allowing allowing a node-node match with any number of edges
 */
public class PathScopeExpression extends ScopeExpression
  {
  public static final PathScopeExpression BLOCKING = new PathScopeExpression( Mode.Blocking );
  public static final PathScopeExpression NON_BLOCKING = new PathScopeExpression( Mode.NonBlocking );

  public static final PathScopeExpression ALL_BLOCKING = new PathScopeExpression( Applies.All, Mode.Blocking );
  public static final PathScopeExpression ALL_NON_BLOCKING = new PathScopeExpression( Applies.All, Mode.NonBlocking );

  public static final PathScopeExpression ANY_BLOCKING = new PathScopeExpression( Applies.Any, Mode.Blocking );
  public static final PathScopeExpression ANY_NON_BLOCKING = new PathScopeExpression( Applies.Any, Mode.NonBlocking );

  public enum Mode
    {
      Ignore, Blocking, NonBlocking
    }

  private Mode mode = Mode.Ignore;

  public PathScopeExpression()
    {
    }

  public PathScopeExpression( Applies applies )
    {
    this.applies = applies;
    }

  public PathScopeExpression( Mode mode )
    {
    this.mode = mode;
    }

  public PathScopeExpression( Applies applies, Mode mode )
    {
    super( applies );
    this.mode = mode;
    }

  @Override
  public boolean acceptsAll()
    {
    return appliesToAllPaths() && isIgnoreMode();
    }

  public boolean isIgnoreMode()
    {
    return mode == Mode.Ignore;
    }

  public Mode getMode()
    {
    return mode;
    }

  @Override
  public boolean applies( PlannerContext plannerContext, ElementGraph elementGraph, Scope scope )
    {
    switch( mode )
      {
      case Ignore:
        return true;
      case Blocking:
        return !scope.isNonBlocking();
      case NonBlocking:
        return scope.isNonBlocking();
      default:
        throw new IllegalStateException( "should never reach here" );
      }
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "PathScopeExpression{" );
    sb.append( "path=" ).append( applies );
    sb.append( ", mode=" ).append( mode );
    sb.append( '}' );
    return sb.toString();
    }
  }
