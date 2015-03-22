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

package cascading.util;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.process.ProcessGraph;
import cascading.flow.planner.process.ProcessModel;
import org.jgrapht.ext.ComponentAttributeProvider;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.VertexNameProvider;

/**
 * This class is a derivative of the JGraphT DOTExporter, with numerous enhancements but with
 * retained compatibility.
 */
public class DOTProcessGraphWriter
  {
  public static final String INDENT = "  ";
  public static final String CONNECTOR = " -> ";

  private VertexNameProvider<Pair<ElementGraph, FlowElement>> vertexIDProvider;
  private VertexNameProvider<FlowElement> vertexLabelProvider;
  private EdgeNameProvider<Scope> edgeLabelProvider;
  private ComponentAttributeProvider<FlowElement> vertexAttributeProvider;
  private ComponentAttributeProvider<Scope> edgeAttributeProvider;
  private VertexNameProvider<ProcessModel> clusterIDProvider;
  private VertexNameProvider<ProcessModel> clusterLabelProvider;

  public DOTProcessGraphWriter( VertexNameProvider<Pair<ElementGraph, FlowElement>> vertexIDProvider, VertexNameProvider<FlowElement> vertexLabelProvider,
                                EdgeNameProvider<Scope> edgeLabelProvider,
                                ComponentAttributeProvider<FlowElement> vertexAttributeProvider, ComponentAttributeProvider<Scope> edgeAttributeProvider,
                                VertexNameProvider<ProcessModel> clusterIDProvider, VertexNameProvider<ProcessModel> clusterLabelProvider )
    {
    this.vertexIDProvider = vertexIDProvider;
    this.vertexLabelProvider = vertexLabelProvider;
    this.edgeLabelProvider = edgeLabelProvider;
    this.vertexAttributeProvider = vertexAttributeProvider;
    this.edgeAttributeProvider = edgeAttributeProvider;
    this.clusterIDProvider = clusterIDProvider;
    this.clusterLabelProvider = clusterLabelProvider;
    }

  public void writeGraph( Writer writer, ElementGraph parentGraph, ProcessGraph<? extends ProcessModel> processGraph )
    {
    PrintWriter out = new PrintWriter( writer );

    out.println( "digraph G {" );

    Set<FlowElement> spanElements = new HashSet<>();

    spanElements.add( Extent.head );
    spanElements.add( Extent.tail );
    spanElements.addAll( processGraph.getAllSourceElements() );
    spanElements.addAll( processGraph.getAllSinkElements() );
    spanElements.removeAll( processGraph.getSourceTaps() );
    spanElements.removeAll( processGraph.getSinkTaps() );

    Set<FlowElement> duplicatedElements = processGraph.getDuplicatedElements( parentGraph );

    writeVertexSet( processGraph, parentGraph, out, spanElements, true, duplicatedElements );
    writeEdgeSet( processGraph, parentGraph, parentGraph, out, spanElements, true );

    Iterator<? extends ProcessModel> topologicalIterator = processGraph.getTopologicalIterator();

    while( topologicalIterator.hasNext() )
      {
      ProcessModel processModel = topologicalIterator.next();

      out.println();
      out.print( "subgraph cluster_" );
      out.print( clusterIDProvider.getVertexName( processModel ) );
      out.println( " {" );

      out.print( INDENT );
      out.print( "label = \"" );
      out.print( clusterLabelProvider.getVertexName( processModel ) );
      out.println( "\";" );
      out.println();

      writeVertexSet( processGraph, processModel.getElementGraph(), out, spanElements, false, duplicatedElements );
      writeEdgeSet( processGraph, parentGraph, processModel.getElementGraph(), out, spanElements, false );

      out.println( "}" );
      }

    out.println( "}" );

    out.flush();
    }

  protected void writeEdgeSet( ProcessGraph<? extends ProcessModel> processGraph, ElementGraph parentGraph, ElementGraph currentGraph, PrintWriter out, Set<FlowElement> spansClusters, boolean onlySpans )
    {
    out.println();

    for( Scope scope : currentGraph.edgeSet() )
      {
      FlowElement edgeSource = currentGraph.getEdgeSource( scope );
      FlowElement edgeTarget = currentGraph.getEdgeTarget( scope );

      boolean sourceSpans = spansClusters.contains( edgeSource );
      boolean targetSpans = spansClusters.contains( edgeTarget );
      boolean spans = sourceSpans || targetSpans;

      if( spans != onlySpans )
        continue;

      List<ElementGraph> sourceGraphs = Arrays.asList( currentGraph );
      List<ElementGraph> targetGraphs = Arrays.asList( currentGraph );

      if( sourceSpans )
        {
        sourceGraphs = Arrays.asList( parentGraph );
        targetGraphs = processGraph.getElementGraphs( edgeTarget );
        }

      if( targetSpans )
        {
        sourceGraphs = processGraph.getElementGraphs( edgeSource );
        targetGraphs = Arrays.asList( parentGraph );
        }

      for( ElementGraph sourceGraph : sourceGraphs )
        {
        for( ElementGraph targetGraph : targetGraphs )
          {
          String source = getVertexID( sourceGraph, edgeSource );
          String target = getVertexID( targetGraph, edgeTarget );

          out.print( INDENT + source + CONNECTOR + target );

          String labelName = null;

          if( edgeLabelProvider != null )
            labelName = edgeLabelProvider.getEdgeName( scope );

          Map<String, String> attributes = null;

          if( edgeAttributeProvider != null )
            attributes = edgeAttributeProvider.getComponentAttributes( scope );

          renderAttributes( out, labelName, attributes );

          out.println( ";" );
          }
        }
      }
    }

  protected void writeVertexSet( ProcessGraph<? extends ProcessModel> processGraph, ElementGraph elementGraph, PrintWriter out, Set<FlowElement> spansClusters, boolean onlySpans, Set<FlowElement> duplicatedElements )
    {
    for( FlowElement flowElement : elementGraph.vertexSet() )
      {
      boolean spans = spansClusters.contains( flowElement );

      if( spans != onlySpans )
        continue;

      out.print( INDENT + getVertexID( elementGraph, flowElement ) );

      String labelName = null;

      if( vertexLabelProvider != null )
        labelName = vertexLabelProvider.getVertexName( flowElement );

      Map<String, String> attributes = new HashMap<>();

      if( duplicatedElements.contains( flowElement ) )
        attributes.put( "color", getHSBColorFor( flowElement ) );

      if( vertexAttributeProvider != null )
        attributes.putAll( vertexAttributeProvider.getComponentAttributes( flowElement ) );

      renderAttributes( out, labelName, attributes );

      out.println( ";" );
      }
    }

  private void renderAttributes( PrintWriter out, String labelName, Map<String, String> attributes )
    {
    if( labelName == null && attributes == null )
      return;

    out.print( " [ " );

    if( labelName == null )
      labelName = attributes.get( "label" );

    if( labelName != null )
      out.print( "label=\"" + labelName + "\" " );

    if( attributes != null )
      {
      for( Map.Entry<String, String> entry : attributes.entrySet() )
        {
        String name = entry.getKey();

        if( name.equals( "label" ) )
          continue;

        out.print( name + "=\"" + entry.getValue() + "\" " );
        }
      }

    out.print( "]" );
    }

  private String getVertexID( ElementGraph elementGraph, FlowElement flowElement )
    {
    return vertexIDProvider.getVertexName( new Pair<>( elementGraph, flowElement ) );
    }

  Map<FlowElement, String> colors = new HashMap<>();
  float hue = 0.3f;

  // a rudimentary attempt to progress the colors so they can be differentiated
  private String getHSBColorFor( FlowElement flowElement )
    {
    if( colors.containsKey( flowElement ) )
      return colors.get( flowElement );

    String result = String.format( "%f,%f,%f", 1.0 - hue % 1.0, 1.0, 0.9 );

    colors.put( flowElement, result );

    hue += 0.075 + ( 0.025 * Math.floor( hue ) );

    return result;
    }
  }
