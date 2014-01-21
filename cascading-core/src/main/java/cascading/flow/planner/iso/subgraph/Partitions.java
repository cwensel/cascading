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
import java.util.List;

import cascading.flow.planner.FlowElementGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.finder.Match;
import cascading.flow.planner.rule.Rule;

/**
 *
 */
public class Partitions
  {
  private final GraphPartitioner graphPartitioner;
  private final SubGraphIterator stepIterator;
  private final FlowElementGraph elementGraph;
  private final List<ElementGraph> subGraphs;

  public Partitions( GraphPartitioner graphPartitioner, SubGraphIterator stepIterator, FlowElementGraph elementGraph, List<ElementGraph> subGraphs )
    {
    this.graphPartitioner = graphPartitioner;
    this.stepIterator = stepIterator;
    this.elementGraph = elementGraph;
    this.subGraphs = subGraphs;
    }

  public GraphPartitioner getGraphPartitioner()
    {
    return graphPartitioner;
    }

  public FlowElementGraph getElementGraph()
    {
    return elementGraph;
    }

  public List<ElementGraph> getSubGraphs()
    {
    return subGraphs;
    }

  public String getRuleName()
    {
    if( getGraphPartitioner() instanceof Rule )
      return ( (Rule) getGraphPartitioner() ).getRuleName();

    return "none";
    }

  public void writeDOTs( String path )
    {
    int count = 0;

    elementGraph.writeDOT( new File( path, "element-graph.dot" ).toString() );

    if( graphPartitioner.getContractionGraph() != null )
      graphPartitioner.getContractionGraph().writeDOT( new File( path, "contraction-graph.dot" ).toString() );

    if( graphPartitioner.getExpressionGraph() != null )
      graphPartitioner.getExpressionGraph().writeDOT( new File( path, "expression-graph.dot" ).toString() );

    List<Match> matches = stepIterator == null ? null : stepIterator.getMatches();

    for( int i = 0; i < subGraphs.size(); i++ )
      {
      ElementGraph subGraph = subGraphs.get( i );

      subGraph.writeDOT( new File( path, makeFileName( count++, "sub-graph", "match" ) ).toString() );

      if( matches != null )
        matches.get( i ).getMatchedGraph().writeDOT( new File( path, makeFileName( count, "matched-graph", "match" ) ).toString() );
      }
    }

  private String makeFileName( int ordinal, String name, String state )
    {
    return String.format( "%02d-%s-%s.dot", ordinal, name, state );
    }
  }
