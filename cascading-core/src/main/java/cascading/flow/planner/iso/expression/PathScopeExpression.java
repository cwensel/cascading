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
 *  All is a special case allowing allowing a node-node match with any number of edges
 */
public class PathScopeExpression extends ScopeExpression
  {
  public static final PathScopeExpression ANY = new PathScopeExpression( Path.Any );
  public static final PathScopeExpression ALL = new PathScopeExpression( Path.All );
  public static final PathScopeExpression BLOCKING = new PathScopeExpression( Path.Blocking );
  public static final PathScopeExpression NON_BLOCKING = new PathScopeExpression( Path.NonBlocking );

  public enum Path
    {
      Any, All, Blocking, NonBlocking
    }

  private Path path = Path.Any;

  public PathScopeExpression()
    {
    }

  public PathScopeExpression( Path path )
    {
    this.path = path;
    }

  public boolean appliesToAll()
    {
    return path == Path.All;
    }

  public Path getPath()
    {
    return path;
    }

  @Override
  public boolean applies( PlannerContext plannerContext, ElementGraph elementGraph, Scope scope )
    {
    switch( path )
      {
      case Any:
      case All:
        return true;
      case Blocking:
        return scope.getOrdinal() != 0;
      case NonBlocking:
        return scope.getOrdinal() == 0;
      default:
        throw new IllegalStateException( "should never reach here" );
      }
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "ScopeExpression{" );
    sb.append( "path=" ).append( path );
    sb.append( '}' );
    return sb.toString();
    }
  }
