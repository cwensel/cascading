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

package cascading.flow.planner.iso.transformer;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementDirectedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.rule.Rule;

/**
 *
 */
public class Transform<E extends ElementGraph>
  {
  PlannerContext plannerContext;
  GraphTransformer graphTransformer;
  ExpressionGraph expressionGraph;
  ElementGraph beginGraph;
  int recursionCount = 0;
  List<ElementGraph> recursions;
  List<Transform> childTransforms;
  E endGraph;

  public Transform( PlannerContext plannerContext, GraphTransformer graphTransformer, ElementGraph beginGraph )
    {
    this.plannerContext = plannerContext;
    this.graphTransformer = graphTransformer;

    if( plannerContext.isTracingEnabled() )
      beginGraph = new ElementDirectedGraph( beginGraph );

    this.beginGraph = beginGraph;
    }

  public Transform( PlannerContext plannerContext, GraphTransformer graphTransformer, ExpressionGraph expressionGraph, ElementGraph beginGraph )
    {
    this.plannerContext = plannerContext;
    this.graphTransformer = graphTransformer;
    this.expressionGraph = expressionGraph;

    if( plannerContext.isTracingEnabled() )
      beginGraph = new ElementDirectedGraph( beginGraph );

    this.beginGraph = beginGraph;
    }

  public PlannerContext getPlannerContext()
    {
    return plannerContext;
    }

  public String getRuleName()
    {
    if( getGraphTransform() instanceof Rule )
      return ( (Rule) getGraphTransform() ).getRuleName();

    return "none";
    }

  public String getTransformerName()
    {
    return getGraphTransform().getClass().getSimpleName();
    }

  public GraphTransformer getGraphTransform()
    {
    return graphTransformer;
    }

  public void setEndGraph( E endGraph )
    {
    this.endGraph = endGraph;
    }

  public E getEndGraph()
    {
    return endGraph;
    }

  public int getNumRecursions()
    {
    return recursionCount;
    }

  public List<ElementGraph> getRecursions()
    {
    if( recursions == null )
      recursions = new LinkedList<>();

    return recursions;
    }

  public List<Transform> getChildTransforms()
    {
    if( childTransforms == null )
      childTransforms = new LinkedList<>();

    return childTransforms;
    }

  void addRecursionTransform( ElementGraph transformed )
    {
    recursionCount++;

    if( plannerContext.isTracingEnabled() )
      getRecursions().add( new ElementDirectedGraph( transformed ) );
    }

  public void addChildTransform( Transform transform )
    {
    if( plannerContext.isTracingEnabled() )
      getChildTransforms().add( transform );
    }

  public void writeDOTs( String path )
    {
    int count = 0;

    if( expressionGraph != null )
      expressionGraph.writeDOT( new File( path, "expression-graph.dot" ).toString() );

    for( int i = 0; i < getChildTransforms().size(); i++ )
      {
      Transform transform = getChildTransforms().get( i );
      String name = transform.getTransformerName();
      transform.writeDOTs( path + "/child-" + i + "-" + name + "/" );
      }

    if( beginGraph != null )
      {
      String name = beginGraph.getClass().getSimpleName();
      beginGraph.writeDOT( new File( path, makeFileName( count++, name, "begin" ) ).toString() );
      }

    for( ElementGraph recursion : getRecursions() )
      {
      String name = recursion.getClass().getSimpleName();//.replaceAll( "(.*)ElementGraph$", "$1" );
      recursion.writeDOT( new File( path, makeFileName( count++, name, "recursion" ) ).toString() );
      }

    if( getEndGraph() != null )
      {
      String name = getEndGraph().getClass().getSimpleName();
      getEndGraph().writeDOT( new File( path, makeFileName( count, name, "end" ) ).toString() );
      }
    }

  private String makeFileName( int ordinal, String name, String state )
    {
    return String.format( "%02d-%s-%s.dot", ordinal, name, state );
    }
  }
