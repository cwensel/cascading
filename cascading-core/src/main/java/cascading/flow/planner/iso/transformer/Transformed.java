/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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
import cascading.flow.planner.iso.GraphResult;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.rule.Rule;

/**
 *
 */
public class Transformed<E extends ElementGraph> extends GraphResult
  {
  PlannerContext plannerContext;
  GraphTransformer graphTransformer;
  ExpressionGraph expressionGraph;
  ElementGraph beginGraph;
  int recursionCount = 0;
  List<ElementGraph> recursions;
  List<Transformed> childTransforms;
  E endGraph;

  public Transformed( PlannerContext plannerContext, GraphTransformer graphTransformer, ElementGraph beginGraph )
    {
    this.plannerContext = plannerContext;
    this.graphTransformer = graphTransformer;

    if( plannerContext.isTransformTracingEnabled() )
      beginGraph = new ElementDirectedGraph( beginGraph );

    this.beginGraph = beginGraph;
    }

  public Transformed( PlannerContext plannerContext, GraphTransformer graphTransformer, ExpressionGraph expressionGraph, ElementGraph beginGraph )
    {
    this.plannerContext = plannerContext;
    this.graphTransformer = graphTransformer;
    this.expressionGraph = expressionGraph;

    if( plannerContext.isTransformTracingEnabled() )
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

  @Override
  public ElementGraph getBeginGraph()
    {
    return beginGraph;
    }

  public void setEndGraph( E endGraph )
    {
    this.endGraph = endGraph;
    }

  @Override
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

  public List<Transformed> getChildTransforms()
    {
    if( childTransforms == null )
      childTransforms = new LinkedList<>();

    return childTransforms;
    }

  void addRecursionTransform( ElementGraph transformed )
    {
    recursionCount++;

    if( plannerContext.isTransformTracingEnabled() )
      getRecursions().add( new ElementDirectedGraph( transformed ) );
    }

  public void addChildTransform( Transformed transformed )
    {
    if( plannerContext.isTransformTracingEnabled() )
      getChildTransforms().add( transformed );
    }

  @Override
  public void writeDOTs( String path )
    {
    int count = 0;

    if( expressionGraph != null )
      {
      String fileName = String.format( "expression-graph-%s.dot", expressionGraph.getClass().getSimpleName() );
      expressionGraph.writeDOT( new File( path, fileName ).toString() );
      }

    for( int i = 0; i < getChildTransforms().size(); i++ )
      {
      Transformed transformed = getChildTransforms().get( i );
      String name = transformed.getTransformerName();
      String pathName = String.format( "%s/child-%d-%s/", path, i, name );
      transformed.writeDOTs( pathName );
      }

    count = writeBeginGraph( path, count );

    for( ElementGraph recursion : getRecursions() )
      {
      String name = recursion.getClass().getSimpleName();
      recursion.writeDOT( new File( path, makeFileName( count++, name, "recursion" ) ).toString() );
      }

    writeEndGraph( path, count );
    }
  }
