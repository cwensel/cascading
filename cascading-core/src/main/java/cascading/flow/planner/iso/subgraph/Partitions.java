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

package cascading.flow.planner.iso.subgraph;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import cascading.flow.planner.graph.ElementDirectedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.GraphResult;
import cascading.flow.planner.iso.finder.Match;
import cascading.flow.planner.iso.subgraph.partitioner.ExpressionGraphPartitioner;
import cascading.flow.planner.rule.RulePartitioner;
import cascading.util.EnumMultiMap;

/**
 *
 */
public class Partitions extends GraphResult
  {
  private RulePartitioner rulePartitioner;
  private final GraphPartitioner graphPartitioner;
  private final ElementGraph beginGraph;
  private final Map<ElementGraph, EnumMultiMap> annotatedSubGraphs;
  private ElementGraph contractedGraph;
  private List<Match> contractedMatches = Collections.emptyList();

  public Partitions( GraphPartitioner graphPartitioner, ElementGraph beginGraph, Map<ElementGraph, EnumMultiMap> annotatedSubGraphs )
    {
    this( graphPartitioner, beginGraph, null, null, annotatedSubGraphs );
    }

  public Partitions( GraphPartitioner graphPartitioner, ElementGraph beginGraph, ElementGraph contractedGraph, List<Match> contractedMatches, Map<ElementGraph, EnumMultiMap> annotatedSubGraphs )
    {
    this.graphPartitioner = graphPartitioner;
    this.beginGraph = beginGraph;

    if( contractedGraph != null )
      this.contractedGraph = contractedGraph;

    if( contractedMatches != null )
      this.contractedMatches = contractedMatches;

    this.annotatedSubGraphs = annotatedSubGraphs;
    }

  public void setRulePartitioner( RulePartitioner rulePartitioner )
    {
    this.rulePartitioner = rulePartitioner;
    }

  public String getRuleName()
    {
    if( rulePartitioner != null )
      return rulePartitioner.getRuleName();

    return "none";
    }

  @Override
  public ElementGraph getBeginGraph()
    {
    return beginGraph;
    }

  @Override
  public ElementGraph getEndGraph()
    {
    return null;
    }

  public Map<ElementGraph, EnumMultiMap> getAnnotatedSubGraphs()
    {
    return annotatedSubGraphs;
    }

  public boolean hasSubGraphs()
    {
    return !annotatedSubGraphs.isEmpty();
    }

  public boolean hasContractedMatches()
    {
    return !contractedMatches.isEmpty();
    }

  public List<ElementGraph> getSubGraphs()
    {
    return new ArrayList<>( annotatedSubGraphs.keySet() );
    }

  @Override
  public void writeDOTs( String path )
    {
    int count = 0;
    beginGraph.writeDOT( new File( path, makeFileName( count++, "element-graph" ) ).toString() );

    if( graphPartitioner instanceof ExpressionGraphPartitioner )
      {
      ExpressionGraphPartitioner expressionGraphPartitioner = (ExpressionGraphPartitioner) graphPartitioner;

      if( expressionGraphPartitioner.getContractionGraph() != null )
        expressionGraphPartitioner.getContractionGraph().writeDOT( new File( path, makeFileName( count++, "contraction-graph" ) ).toString() );

      if( expressionGraphPartitioner.getExpressionGraph() != null )
        expressionGraphPartitioner.getExpressionGraph().writeDOT( new File( path, makeFileName( count++, "expression-graph" ) ).toString() );
      }

    if( contractedGraph != null )
      contractedGraph.writeDOT( new File( path, makeFileName( count++, "contracted-graph" ) ).toString() );

    List<ElementGraph> subGraphs = getSubGraphs();

    for( int i = 0; i < subGraphs.size(); i++ )
      {
      ElementGraph subGraph = subGraphs.get( i );

      // want to write annotations with elements
      new ElementDirectedGraph( subGraph, annotatedSubGraphs.get( subGraph ) ).writeDOT( new File( path, makeFileName( count, i, "partition-result-sub-graph" ) ).toString() );

      if( i < contractedMatches.size() )
        contractedMatches.get( i ).getMatchedGraph().writeDOT( new File( path, makeFileName( count, i, "partition-contracted-graph" ) ).toString() );
      }
    }

  private String makeFileName( int ordinal, String name )
    {
    return String.format( "%02d-%s.dot", ordinal, name );
    }

  private String makeFileName( int order, int ordinal, String name )
    {
    return String.format( "%02d-%04d-%s.dot", order, ordinal, name );
    }
  }
