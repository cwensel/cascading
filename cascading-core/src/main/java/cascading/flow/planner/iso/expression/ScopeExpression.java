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

import cascading.flow.planner.Scope;

/**
 * If ScopeExpression is the only edge, the {@link ScopeExpression.Applies} enum will apply,
 * otherwise the number of edges on the target graph must match the number of edges in the expression.
 */
public abstract class ScopeExpression implements Expression<Scope>
  {
  /**
   * Where this expression applies between ANY edge between two nodes.
   */
  public static final PathScopeExpression ANY = new PathScopeExpression( Applies.Any );
  /**
   * Where this expression applies between ALL edges between two nodes.
   */
  public static final PathScopeExpression ALL = new PathScopeExpression( Applies.All );
  public static final PathScopeExpression EACH = new PathScopeExpression( Applies.Each ); // unsupported

  /**
   * Match the edge, but do not capture it. Only works if the edge matched in the contracted graph is exactly
   * present in the original graph. If the edge is a contraction of a more complex path, the edges/path won't be
   * discarded.
   */
  public static final PathScopeExpression NO_CAPTURE = new PathScopeExpression( false, Applies.All );

  public enum Applies
    {
      /**
       * At least one edge
       */
      Any,

      /**
       * All edges
       */
      All,

      /**
       * Each edge - unsupported
       */
      Each
    }

  protected boolean capture = true;
  protected Applies applies = Applies.Any;

  protected ScopeExpression()
    {
    }

  protected ScopeExpression( Applies applies )
    {
    this.applies = applies;
    }

  public ScopeExpression( boolean capture, Applies applies )
    {
    this.capture = capture;
    this.applies = applies;

    if( capture == false && applies != Applies.All )
      throw new IllegalArgumentException( "applies must be ALL if capture is false" );
    }

  public boolean isCapture()
    {
    return capture;
    }

  /**
   * This match must apply to all the edges between the two candidate nodes for the match to be true.
   */
  public boolean appliesToAllPaths()
    {
    return applies == Applies.All;
    }

  /**
   * This match must apply to at least one edge between the two candidate nodes for the match to be true.
   * <p>
   * The first to apply is captured.
   */
  public boolean appliesToAnyPath()
    {
    return applies == Applies.Any;
    }

  /**
   * This match is applied to each edge, at least one edge between the two candidate nodes must apply for the match
   * to be true.
   * <p>
   * Each edge that applies is captured.
   */
  public boolean appliesToEachPath()
    {
    return applies == Applies.Each;
    }

  /**
   * True if there is at least one edge between the candidate nodes.
   */
  public boolean acceptsAll()
    {
    return appliesToAllPaths();
    }

  public Applies getApplies()
    {
    return applies;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "ScopeExpression{" );
    sb.append( "capture=" ).append( capture );
    sb.append( ", applies=" ).append( applies );
    sb.append( '}' );
    return sb.toString();
    }
  }
