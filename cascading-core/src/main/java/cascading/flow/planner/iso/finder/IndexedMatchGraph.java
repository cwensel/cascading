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

package cascading.flow.planner.iso.finder;

import java.util.HashSet;
import java.util.Set;

import cascading.flow.planner.iso.expression.ElementExpression;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.ScopeExpression;
import org.jgrapht.DirectedGraph;

/**
 *
 */
class IndexedMatchGraph extends IndexedGraph<DirectedGraph<ElementExpression, ScopeExpression>, ElementExpression, ScopeExpression>
  {
  public IndexedMatchGraph( DirectedGraph<ElementExpression, ScopeExpression> graph )
    {
    super( graph );
    }

  @Override
  public Set<ScopeExpression> getAllEdges( Object sourceVertex, Object targetVertex )
    {
    Set<ScopeExpression> allEdges = super.getAllEdges( sourceVertex, targetVertex );

    Set<ScopeExpression> results = new HashSet<>( allEdges.size() );

    for( ScopeExpression edge : allEdges )
      results.add( ExpressionGraph.unwind( edge ) );

    return results;
    }
  }
