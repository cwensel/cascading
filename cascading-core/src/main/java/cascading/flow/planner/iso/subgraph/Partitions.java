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
import java.util.List;
import java.util.Map;

import cascading.flow.planner.graph.AnnotatedElementSet;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.GraphResult;
import cascading.flow.planner.iso.finder.Match;
import cascading.flow.planner.rule.Rule;

/**
 *
 */
public class Partitions extends GraphResult
  {
  private final GraphPartitioner graphPartitioner;
  private final SubGraphIterator subGraphIterator;
  private final ElementGraph beginGraph;
  private final Map<ElementGraph, AnnotatedElementSet> annotatedSubGraphs;

  public Partitions( GraphPartitioner graphPartitioner, ElementGraph beginGraph, Map<ElementGraph, AnnotatedElementSet> annotatedSubGraphs )
    {
    this( graphPartitioner, null, beginGraph, annotatedSubGraphs );
    }

  public Partitions( GraphPartitioner graphPartitioner, SubGraphIterator subGraphIterator, ElementGraph beginGraph, Map<ElementGraph, AnnotatedElementSet> annotatedSubGraphs )
    {
    this.graphPartitioner = graphPartitioner;
    this.subGraphIterator = subGraphIterator;
    this.beginGraph = beginGraph;
    this.annotatedSubGraphs = annotatedSubGraphs;
    }

  public String getRuleName()
    {
    if( getGraphPartitioner() instanceof Rule )
      return ( (Rule) getGraphPartitioner() ).getRuleName();

    return "none";
    }

  public GraphPartitioner getGraphPartitioner()
    {
    return graphPartitioner;
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

  public Map<ElementGraph, AnnotatedElementSet> getAnnotatedSubGraphs()
    {
    return annotatedSubGraphs;
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

    if( graphPartitioner.getContractionGraph() != null )
      graphPartitioner.getContractionGraph().writeDOT( new File( path, makeFileName( count++, "contraction-graph" ) ).toString() );

    if( graphPartitioner.getExpressionGraph() != null )
      graphPartitioner.getExpressionGraph().writeDOT( new File( path, makeFileName( count++, "expression-graph" ) ).toString() );

    if( subGraphIterator != null )
      subGraphIterator.getContractedGraph().writeDOT( new File( path, makeFileName( count++, "contracted-graph" ) ).toString() );

    List<Match> matches = subGraphIterator == null ? null : subGraphIterator.getContractedMatches();

    List<ElementGraph> subGraphs = getSubGraphs();

    for( int i = 0; i < subGraphs.size(); i++ )
      {
      ElementGraph subGraph = subGraphs.get( i );

      subGraph.writeDOT( new File( path, makeFileName( count, i, "partition-result-sub-graph" ) ).toString() );

      if( matches != null )
        matches.get( i ).getMatchedGraph().writeDOT( new File( path, makeFileName( count, i, "partition-contracted-graph" ) ).toString() );
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
