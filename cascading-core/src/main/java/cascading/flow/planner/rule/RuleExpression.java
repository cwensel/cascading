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

package cascading.flow.planner.rule;

import cascading.flow.planner.iso.expression.ExpressionGraph;

/**
 * A RuleExpression describes where a {@link cascading.flow.planner.rule.Rule} is applied in an element graph. Where
 * a Rule can be a {@link cascading.flow.planner.rule.RuleAssert}, {@link cascading.flow.planner.rule.RuleTransformer},
 * or {@link cascading.flow.planner.rule.RulePartitioner}.
 * <p/>
 * To pin down how a Rule behaves and where, a RuleExpression relies on the
 * {@link cascading.flow.planner.iso.expression.ExpressionGraph} class, where an ExpressionGraph is an actual graph
 * of {@link cascading.flow.planner.iso.expression.ElementExpression} nodes and
 * {@link cascading.flow.planner.iso.expression.ScopeExpression} edges.
 * <p/>
 * This expression graph is analogous to a text Regular Expression. A regular expression is used to match a sub-set
 * of text in a string. This enables efficient string replacement and parsing.
 * <p/>
 * The expression graph is used to match a smaller graph inside a larger one so that the larger graph can be
 * manipulated. As in a regular expression, elements of the captured graph can be addressed and used.
 * <p/>
 * The simplest application is to use a single ExpressionGraph to match a portion of a larger graph. Once found,
 * the calling rule can fire. Most commonly this is useful for the RuleAssert rule that looks for a given structure,
 * and if founds throws an error pinpointing the element in the graph that violates the assertion rule.
 * <p/>
 * The second application is to have one ExpressionGraph identify distinguished elements in a larger graph, and
 * remove them. This is called graph contraction. The structure of the original graph is retained where possible.
 * Think of this as hiding elements in the larger graph so that a second ExpressionGraph can be applied to look
 * for a sub-graph.
 * <p/>
 * If the second sub-graph is found, the calling Rule can execute. This most commonly used to partition a graph into
 * smaller graphs. For example, any sub-graph with source Taps and a single sink Group (Pipe) is a process Node (Mapper).
 * <p/>
 * The third application is similar to the second. An ExpressionGraph is used to create a contracted graph of only
 * distinguished elements. The second ExpressionGraph finds a sub-graph in the contracted graph. This contracted
 * sub-graph is then isolated and all hidden elements are restored within the bounds of the sub-graph.
 * <p/>
 * Finally a third ExpressionGraph is used to identify a location within the new sub-graph so the Rule can execute.
 * This is most commonly used to perform transformations within a graph. For example, to insert a temporary Tap into
 * the full assembly element graph to force boundaries between MapReduce jobs.
 */
public class RuleExpression
  {
  protected final ExpressionGraph contractionExpression;
  protected final ExpressionGraph contractedMatchExpression;
  protected final ExpressionGraph matchExpression;

  public RuleExpression( ExpressionGraph matchExpression )
    {
    this.contractionExpression = null;
    this.contractedMatchExpression = null;
    this.matchExpression = matchExpression;

    verify();
    }

  public RuleExpression( ExpressionGraph contractionExpression, ExpressionGraph matchExpression )
    {
    this.contractionExpression = contractionExpression;
    this.contractedMatchExpression = null;
    this.matchExpression = matchExpression;

    verify();
    }

  public RuleExpression( ExpressionGraph contractionExpression, ExpressionGraph contractedMatchExpression, ExpressionGraph matchExpression )
    {
    this.contractionExpression = contractionExpression;
    this.contractedMatchExpression = contractedMatchExpression;
    this.matchExpression = matchExpression;

    verify();
    }

  private void verify()
    {
    // test for
    // constraction removes
    // contractedMatch
    // matchExpression inserts
    }

  public String getExpressionName()
    {
    return getClass().getSimpleName().replaceAll( "^(.*)[]A-Z][a-z]*Rule$", "$1" );
    }

  public ExpressionGraph getContractionExpression()
    {
    return contractionExpression;
    }

  public ExpressionGraph getContractedMatchExpression()
    {
    return contractedMatchExpression;
    }

  public ExpressionGraph getMatchExpression()
    {
    return matchExpression;
    }
  }
