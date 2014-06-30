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

package cascading.flow.planner.iso.assertion;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementSubGraph;
import cascading.flow.planner.iso.GraphResult;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.finder.Match;
import cascading.flow.planner.iso.transformer.Transformed;
import cascading.flow.planner.rule.Rule;
import cascading.util.Util;

/**
 *
 */
public class Asserted extends GraphResult
  {
  PlannerContext plannerContext;
  GraphAssert graphAssert;
  ElementGraph beginGraph;
  final String message;
  final Match match;

  List<Transformed> childTransforms;

  public Asserted( PlannerContext plannerContext, GraphAssert graphAssert, ElementGraph beginGraph, String message, Match match )
    {
    this.plannerContext = plannerContext;
    this.graphAssert = graphAssert;
    this.beginGraph = beginGraph;
    this.message = message;
    this.match = match;
    }

  public GraphAssert getGraphAssert()
    {
    return graphAssert;
    }

  @Override
  public ElementGraph getBeginGraph()
    {
    return beginGraph;
    }

  public String getMessage()
    {
    String result = message;

    for( ElementCapture capture : ElementCapture.values() )
      {
      Iterator<FlowElement> iterator = match.getCapturedElements( capture ).iterator();

      while( result.contains( "{" + capture + "}" ) && iterator.hasNext() )
        result = result.replaceFirst( "\\{" + capture + "\\}", iterator.next().toString() );
      }

    return result;
    }

  public String getRuleName()
    {
    if( getGraphAssert() instanceof Rule )
      return ( (Rule) getGraphAssert() ).getRuleName();

    return "none";
    }

  public ElementSubGraph getMatched()
    {
    return match.getMatchedGraph();
    }

  public Set<FlowElement> getAnchors()
    {
    return match.getCapturedElements( ElementCapture.Primary );
    }

  public FlowElement getFirstAnchor()
    {
    return Util.getFirst( getAnchors() );
    }

  @Override
  public ElementGraph getEndGraph()
    {
    return null;
    }

  public List<Transformed> getChildTransforms()
    {
    if( childTransforms == null )
      childTransforms = new LinkedList<>();

    return childTransforms;
    }

  public void addChildTransform( Transformed transformed )
    {
    if( plannerContext.isTracingEnabled() )
      getChildTransforms().add( transformed );
    }

  @Override
  public void writeDOTs( String path )
    {
    int count = 0;

    for( int i = 0; i < getChildTransforms().size(); i++ )
      {
      Transformed transformed = getChildTransforms().get( i );
      String name = transformed.getTransformerName();
      transformed.writeDOTs( path + "/child-" + i + "-" + name + "/" );
      }

    count = writeBeginGraph( path, count );

    writeEndGraph( path, count );
    }
  }
