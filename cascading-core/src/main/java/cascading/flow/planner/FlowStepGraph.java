/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowStep;
import cascading.pipe.Group;
import cascading.tap.Tap;
import cascading.util.Util;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * @param elementGraph of type ElementGraph
   */
  public FlowStepGraph( String flowName, ElementGraph elementGraph )
    {
    this();

    makeStepGraph( flowName, elementGraph );
    }

  /**
   * Method getCreateFlowStep ...
   *
   * @param steps   of type Map<String, FlowStep>
   * @param sink    of type String
   * @param numJobs of type int
   * @return FlowStep
   */
  protected FlowStep<Config> getCreateFlowStep( Map<Tap, FlowStep<Config>> steps, Tap sink, int numJobs )
    {
    if( steps.containsKey( sink ) )
      return steps.get( sink );

    LOG.debug( "creating step: {}", sink );

    int stepNum = steps.size() + 1;
    String stepName = makeStepName( sink, numJobs, stepNum );
    FlowStep<Config> step = createFlowStep( stepName, stepNum );

    steps.put( sink, step );

    return step;
    }

  protected abstract FlowStep<Config> createFlowStep( String stepName, int stepNum );

  private String makeStepName( Tap sink, int numJobs, int stepNum )
    {
    if( sink.isTemporary() )
      return String.format( "(%d/%d)", stepNum, numJobs );

    String identifier = sink.getIdentifier();

    if( identifier.length() > 25 )
      identifier = String.format( "...%25s", identifier.substring( identifier.length() - 25 ) );

    return String.format( "(%d/%d) %s", stepNum, numJobs, identifier );
    }

  protected abstract void makeStepGraph( String flowName, ElementGraph elementGraph );

  protected boolean pathContainsTap( GraphPath<FlowElement, Scope> path )
    {
    List<FlowElement> flowElements = Graphs.getPathVertexList( path );

    // first and last are taps, if we find more than 2, return false
    int count = 0;

    for( FlowElement flowElement : flowElements )
      {
      if( flowElement instanceof Tap )
        count++;
      }

    return count > 2;
    }

  public TopologicalOrderIterator<FlowStep<Config>, Integer> getTopologicalIterator()
    {
    return new TopologicalOrderIterator<FlowStep<Config>, Integer>( this, new PriorityQueue<FlowStep<Config>>( 10, new Comparator<FlowStep<Config>>()
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
        String sourceName = "";

        for( Object object : flowStep.getSources() )
          {
          Tap source = (Tap) object;

          if( source.isTemporary() )
            continue;

          sourceName += "[" + source.getIdentifier() + "]";
          }

        String name = "[" + flowStep.getName() + "]";

        if( sourceName.length() != 0 )
          name += "\\nsrc:" + sourceName;


        List<Group> groups = flowStep.getGroups();

        for( Group group : groups )
          {
          String groupName = group.getName();

          if( groupName.length() != 0 )
            name += "\\ngrp:" + groupName;
          }

        Set<Tap> sinks = flowStep.getSinks();

        for( Tap sink : sinks )
          {
          String sinkName = sink.isTemporary() ? "" : "[" + sink.getIdentifier() + "]";
          if( sinkName.length() != 0 )
            name += "\\nsnk:" + sinkName;
          }

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
