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

/*
 * See package LICENSE.txt for additional license information.
 */

package cascading.util.jgrapht;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.Map;

import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;

public class DOTExporter<V, E>
  {
  private VertexNameProvider<V> vertexIDProvider;
  private VertexNameProvider<V> vertexLabelProvider;
  private EdgeNameProvider<E> edgeLabelProvider;
  private ComponentAttributeProvider<V> vertexAttributeProvider;
  private ComponentAttributeProvider<E> edgeAttributeProvider;

  /**
   * Constructs a new DOTExporter object with an integer name provider for the
   * vertex IDs and null providers for the vertex and edge labels.
   */
  public DOTExporter()
    {
    this( new IntegerNameProvider<V>(), null, null );
    }

  /**
   * Constructs a new DOTExporter object with the given ID and label
   * providers.
   *
   * @param vertexIDProvider    for generating vertex IDs. Must not be null.
   * @param vertexLabelProvider for generating vertex labels. If null, vertex
   *                            labels will not be written to the file.
   * @param edgeLabelProvider   for generating edge labels. If null, edge labels
   *                            will not be written to the file.
   */
  public DOTExporter(
    VertexNameProvider<V> vertexIDProvider,
    VertexNameProvider<V> vertexLabelProvider,
    EdgeNameProvider<E> edgeLabelProvider )
    {
    this(
      vertexIDProvider,
      vertexLabelProvider,
      edgeLabelProvider,
      null,
      null );
    }

  /**
   * Constructs a new DOTExporter object with the given ID, label, and
   * attribute providers. Note that if a label provider conflicts with a
   * label-supplying attribute provider, the label provider is given
   * precedence.
   *
   * @param vertexIDProvider        for generating vertex IDs. Must not be null.
   * @param vertexLabelProvider     for generating vertex labels. If null, vertex
   *                                labels will not be written to the file (unless an attribute provider is
   *                                supplied which also supplies labels).
   * @param edgeLabelProvider       for generating edge labels. If null, edge labels
   *                                will not be written to the file.
   * @param vertexAttributeProvider for generating vertex attributes. If null,
   *                                vertex attributes will not be written to the file.
   * @param edgeAttributeProvider   for generating edge attributes. If null,
   *                                edge attributes will not be written to the file.
   */
  public DOTExporter(
    VertexNameProvider<V> vertexIDProvider,
    VertexNameProvider<V> vertexLabelProvider,
    EdgeNameProvider<E> edgeLabelProvider,
    ComponentAttributeProvider<V> vertexAttributeProvider,
    ComponentAttributeProvider<E> edgeAttributeProvider )
    {
    this.vertexIDProvider = vertexIDProvider;
    this.vertexLabelProvider = vertexLabelProvider;
    this.edgeLabelProvider = edgeLabelProvider;
    this.vertexAttributeProvider = vertexAttributeProvider;
    this.edgeAttributeProvider = edgeAttributeProvider;
    }

  /**
   * Exports a graph into a plain text file in DOT format.
   *
   * @param writer the writer to which the graph to be exported
   * @param g      the graph to be exported
   */
  public void export( Writer writer, Graph<V, E> g )
    {
    PrintWriter out = new PrintWriter( writer );
    String indent = "  ";
    String connector;

    if( g instanceof DirectedGraph<?, ?> )
      {
      out.println( "digraph G {" );
      connector = " -> ";
      }
    else
      {
      out.println( "graph G {" );
      connector = " -- ";
      }

    for( V v : g.vertexSet() )
      {
      out.print( indent + getVertexID( v ) );

      String labelName = null;
      if( vertexLabelProvider != null )
        {
        labelName = vertexLabelProvider.getVertexName( v );
        }
      Map<String, String> attributes = null;
      if( vertexAttributeProvider != null )
        {
        attributes = vertexAttributeProvider.getComponentAttributes( v );
        }
      renderAttributes( out, labelName, attributes );

      out.println( ";" );
      }

    for( E e : g.edgeSet() )
      {
      String source = getVertexID( g.getEdgeSource( e ) );
      String target = getVertexID( g.getEdgeTarget( e ) );

      out.print( indent + source + connector + target );

      String labelName = null;
      if( edgeLabelProvider != null )
        {
        labelName = edgeLabelProvider.getEdgeName( e );
        }
      Map<String, String> attributes = null;
      if( edgeAttributeProvider != null )
        {
        attributes = edgeAttributeProvider.getComponentAttributes( e );
        }
      renderAttributes( out, labelName, attributes );

      out.println( ";" );
      }

    out.println( "}" );

    out.flush();
    }

  private void renderAttributes(
    PrintWriter out,
    String labelName,
    Map<String, String> attributes )
    {
    if( ( labelName == null ) && ( attributes == null ) )
      {
      return;
      }
    out.print( " [ " );
    if( ( labelName == null ) && ( attributes != null ) )
      {
      labelName = attributes.get( "label" );
      }
    if( labelName != null )
      {
      out.print( "label=\"" + labelName + "\" " );
      }
    if( attributes != null )
      {
      for( Map.Entry<String, String> entry : attributes.entrySet() )
        {
        String name = entry.getKey();
        if( name.equals( "label" ) )
          {
          // already handled by special case above
          continue;
          }
        out.print( name + "=\"" + entry.getValue() + "\" " );
        }
      }
    out.print( "]" );
    }

  /**
   * Return a valid vertex ID (with respect to the .dot language definition as
   * described in http://www.graphviz.org/doc/info/lang.html Quoted from above
   * mentioned source: An ID is valid if it meets one of the following
   * criteria:
   * <p/>
   * <ul>
   * <li>any string of alphabetic characters, underscores or digits, not
   * beginning with a digit;
   * <li>a number [-]?(.[0-9]+ | [0-9]+(.[0-9]*)? );
   * <li>any double-quoted string ("...") possibly containing escaped quotes
   * (\");
   * <li>an HTML string (<...>).
   * </ul>
   *
   * @throws RuntimeException if the given <code>vertexIDProvider</code>
   *                          didn't generate a valid vertex ID.
   */
  private String getVertexID( V v )
    {
    // TODO jvs 28-Jun-2008:  possible optimizations here are
    // (a) only validate once per vertex
    // (b) compile regex patterns

    // use the associated id provider for an ID of the given vertex
    String idCandidate = vertexIDProvider.getVertexName( v );

    // now test that this is a valid ID
    boolean isAlphaDig = idCandidate.matches( "[a-zA-Z]+([\\w_]*)?" );
    boolean isDoubleQuoted = idCandidate.matches( "\".*\"" );
    boolean isDotNumber =
      idCandidate.matches( "[-]?([.][0-9]+|[0-9]+([.][0-9]*)?)" );
    boolean isHTML = idCandidate.matches( "<.*>" );

    if( isAlphaDig || isDotNumber || isDoubleQuoted || isHTML )
      {
      return idCandidate;
      }

    throw new RuntimeException(
      "Generated id '" + idCandidate + "'for vertex '" + v
        + "' is not valid with respect to the .dot language" );
    }
  }
