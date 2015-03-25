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

package cascading.flow.planner.graph;

import java.util.Collection;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.pipe.Pipe;
import org.jgrapht.DirectedGraph;

/**
 * Interface ElementGraph holds a directed acyclic graph of {@link FlowElement} instances.
 * <p/>
 * An element graph represents an assembly of {@link Pipe} instances bound to source and sink
 * {@link cascading.tap.Tap} instances.
 * <p/>
 * Typically an element graph is fed to a {@link cascading.flow.planner.FlowPlanner} which in return creates
 * a collection of new element graph instances that represent an executable version of the original graph.
 * <p/>
 * During the planning, multiple intermediate element graphs will be created representing the full graph down to
 * elemental sub-graphs.
 * <p/>
 * There are many concrete implementations of the ElementGraph interface to support this process.
 * <p/>
 * Frequently an element graph will have a single head represented by the singleton {@link Extent#head} and
 * a single tail represented by the singleton {@link Extent#tail}. These are markers to improve the navigability
 * of the element graph. They can be simply masked by wrapping an element graph instance with a
 * {@link ElementMaskSubGraph} class.
 * <p/>
 * An element graph may be annotated if it implements the {@link AnnotatedGraph} interface.
 * <p/>
 * Annotated graphs have FlowElement instances that may have {@link Enum} instances associated with them. A given
 * planner may have rules that apply annotations to various elements that inform a given platform during execuction
 * or downstream rules in the planner {@link cascading.flow.planner.rule.RuleRegistry}.
 * <p/>
 * Any element graph can be written to a DOT file format for visualization via the {@link #writeDOT(String)} method.
 *
 * @see ElementDirectedGraph
 * @see ElementSubGraph
 * @see ElementMultiGraph
 * @see ElementMaskSubGraph
 * @see ElementGraphs
 */
public interface ElementGraph extends DirectedGraph<FlowElement, Scope>
  {
  Set<Scope> getAllEdges( FlowElement lhs, FlowElement rhs );

  Scope getEdge( FlowElement lhs, FlowElement rhs );

  Scope addEdge( FlowElement lhs, FlowElement rhs );

  boolean addEdge( FlowElement lhs, FlowElement rhs, Scope scope );

  boolean addVertex( FlowElement flowElement );

  boolean containsEdge( FlowElement lhs, FlowElement rhs );

  boolean containsEdge( Scope scope );

  boolean containsVertex( FlowElement flowElement );

  Set<Scope> edgeSet();

  Set<Scope> edgesOf( FlowElement flowElement );

  boolean removeAllEdges( Collection<? extends Scope> scopes );

  Set<Scope> removeAllEdges( FlowElement lhs, FlowElement rhs );

  boolean removeAllVertices( Collection<? extends FlowElement> flowElements );

  Scope removeEdge( FlowElement lhs, FlowElement rhs );

  boolean removeEdge( Scope scope );

  boolean removeVertex( FlowElement flowElement );

  Set<FlowElement> vertexSet();

  FlowElement getEdgeSource( Scope scope );

  FlowElement getEdgeTarget( Scope scope );

  int inDegreeOf( FlowElement flowElement );

  Set<Scope> incomingEdgesOf( FlowElement flowElement );

  int outDegreeOf( FlowElement flowElement );

  Set<Scope> outgoingEdgesOf( FlowElement flowElement );

  ElementGraph copyGraph();

  void writeDOT( String filename );
  }