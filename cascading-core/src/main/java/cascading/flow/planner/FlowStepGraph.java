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

package cascading.flow.planner;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowStep;
import cascading.flow.planner.graph.ElementGraph;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.util.Util;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.planner.ElementGraphs.*;
import static cascading.util.Util.getFirst;

/** Class StepGraph is an internal representation of {@link FlowStep} instances. */
public abstract class FlowStepGraph<Config> extends SimpleDirectedGraph<FlowStep<Config>, Integer>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( FlowStepGraph.class );

  /** Constructor StepGraph creates a new StepGraph instance. */
  public FlowStepGraph()
    {
    super( Integer.class );
    }

  /**
   * Constructor StepGraph creates a new StepGraph instance.
   *
   * @param flowElementGraph of type ElementGraph
   */
  public FlowStepGraph( FlowElementGraph flowElementGraph, List<ElementGraph> elementSubGraphs )
    {
    this();

    makeStepGraph( flowElementGraph, elementSubGraphs );
    }

  protected abstract FlowStep<Config> createFlowStep( String stepName, int stepNum, ElementGraph elementSubGraph );

  /**
   * Creates the map reduce step graph.
   *
   * @param flowElementGraph
   * @param elementSubGraphs
   */
  protected void makeStepGraph( FlowElementGraph flowElementGraph, List<ElementGraph> elementSubGraphs )
    {
    Map<FlowStep, Set<Tap>> stepToSources = new LinkedHashMap<>();
    Map<FlowStep, Set<Tap>> stepToSinks = new LinkedHashMap<>();

    int stepCount = 0;
    for( ElementGraph elementSubGraph : elementSubGraphs )
      {
      if( System.getProperty( FlowPlanner.TRACE_PLAN_PATH ) != null )
        elementSubGraph.writeDOT( System.getProperty( FlowPlanner.TRACE_PLAN_PATH ) + "/steps/" + stepCount++ + "-step-sub-graph.dot" );

      Set<Tap> sources = findSources( elementSubGraph );
      Set<Tap> sinks = findSinks( elementSubGraph );

      String stepName = makeStepName( getFirst( sinks ), elementSubGraphs.size(), vertexSet().size() + 1 );

      FlowStep<Config> flowStep = createFlowStep( stepName, vertexSet().size() + 1, elementSubGraph );

      stepToSources.put( flowStep, new HashSet<>( sources ) );
      stepToSinks.put( flowStep, new HashSet<>( sinks ) );

      addSources( (BaseFlowStep) flowStep, elementSubGraph, sources );
      addSinks( (BaseFlowStep) flowStep, elementSubGraph, sinks );
      ( (BaseFlowStep) flowStep ).addGroups( findAllGroups( elementSubGraph ) );

      assignTraps( (BaseFlowStep) flowStep, flowElementGraph.getTrapMap() );
      addSourceModes( (BaseFlowStep) flowStep );

      addVertex( flowStep );
      }

    int count = 0;
    for( Map.Entry<FlowStep, Set<Tap>> sinkEntry : stepToSinks.entrySet() )
      {
      for( Tap sink : sinkEntry.getValue() )
        {
        for( Map.Entry<FlowStep, Set<Tap>> sourceEntry : stepToSources.entrySet() )
          {
          if( sourceEntry.getKey() == sinkEntry.getKey() )
            continue;

          if( sourceEntry.getValue().contains( sink ) )
            addEdge( sinkEntry.getKey(), sourceEntry.getKey(), count++ );
          }
        }
      }
    }

  protected String makeStepName( Tap sink, int numJobs, int stepNum )
    {
    if( sink.isTemporary() )
      return String.format( "(%d/%d)", stepNum, numJobs );

    String identifier = sink.getIdentifier();

    if( identifier.length() > 25 )
      identifier = String.format( "...%25s", identifier.substring( identifier.length() - 25 ) );

    return String.format( "(%d/%d) %s", stepNum, numJobs, identifier );
    }

  private void assignTraps( BaseFlowStep step, Map<String, Tap> traps )
    {
    for( FlowElement flowElement : step.getGraph().vertexSet() )
      {
      if( !( flowElement instanceof Pipe ) )
        continue;

      String name = ( (Pipe) flowElement ).getName();

      // this is legacy, can probably now collapse into one collection safely
      if( traps.containsKey( name ) )
        step.getTrapMap().put( name, traps.get( name ) );
      }
    }

  private void addSourceModes( BaseFlowStep step )
    {
    Set<HashJoin> hashJoins = ElementGraphs.findAllHashJoins( step.getGraph() );

    for( HashJoin hashJoin : hashJoins )
      {
      for( Object object : step.getSources() )
        {
        Tap source = (Tap) object;
        Map<Integer, Integer> sourcePaths = countOrderedDirectPathsBetween( step.getGraph(), source, hashJoin );

        boolean isStreamed = isOnlyStreamedPath( sourcePaths );
        boolean isAccumulated = isOnlyAccumulatedPath( sourcePaths );
        boolean isBoth = isBothAccumulatedAndStreamedPath( sourcePaths );

        if( isStreamed || isBoth )
          step.addStreamedSourceFor( hashJoin, source );

        if( isAccumulated || isBoth )
          step.addAccumulatedSourceFor( hashJoin, source );
        }
      }
    }

  public TopologicalOrderIterator<FlowStep<Config>, Integer> getTopologicalIterator()
    {
    return new TopologicalOrderIterator<>( this, new PriorityQueue<>( 10, new Comparator<FlowStep<Config>>()
    {
    @Override
    public int compare( FlowStep<Config> lhs, FlowStep<Config> rhs )
      {
      return Integer.valueOf( lhs.getSubmitPriority() ).compareTo( rhs.getSubmitPriority() );
      }
    } ) );
    }

  /**
   * Method writeDOT writes this element graph to a DOT file for easy visualization and debugging.
   *
   * @param filename of type String
   */
  public void writeDOT( String filename )
    {
    printElementGraph( filename );
    }

  protected void printElementGraph( String filename )
    {
    try
      {
      Writer writer = new FileWriter( filename );

      Util.writeDOT( writer, this, new IntegerNameProvider<BaseFlowStep>(), new VertexNameProvider<FlowStep>()
      {
      public String getVertexName( FlowStep flowStep )
        {
        String name = "[" + flowStep.getName() + "]";

        String sourceName = "";
        Set<Tap> sources = flowStep.getSources();
        for( Tap source : sources )
          sourceName += "\\nsrc:[" + source.getIdentifier() + "]";

        if( sourceName.length() != 0 )
          name += sourceName;

        List<Group> groups = flowStep.getGroups();

        for( Group group : groups )
          {
          String groupName = group.getName();

          if( groupName.length() != 0 )
            name += "\\ngrp:" + groupName;
          }

        Set<Tap> sinks = flowStep.getSinks();
        String sinkName = "";
        for( Tap sink : sinks )
          sinkName = "\\nsnk:[" + sink.getIdentifier() + "]";

        if( sinkName.length() != 0 )
          name += sinkName;

        return name.replaceAll( "\"", "\'" );
        }
      }, null );

      writer.close();
      }
    catch( IOException exception )
      {
      LOG.error( "failed printing graph to: {}, with exception: {}", filename, exception );
      }
    }
  }
