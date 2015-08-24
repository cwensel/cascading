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

package cascading.flow.planner.process;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.tap.Tap;
import cascading.util.EnumMultiMap;

/**
 *
 */
public interface ProcessGraph<Process extends ProcessModel> extends Serializable
  {
  boolean addVertex( Process process );

  Set<ProcessEdge> getAllEdges( Process lhs, Process rhs );

  ProcessEdge getEdge( Process lhs, Process rhs );

  ProcessEdge addEdge( Process lhs, Process rhs );

  boolean addEdge( Process lhs, Process rhs, ProcessEdge processEdge );

  boolean containsEdge( Process lhs, Process rhs );

  boolean containsEdge( ProcessEdge processEdge );

  boolean containsVertex( Process process );

  Set<ProcessEdge> edgeSet();

  Set<ProcessEdge> edgesOf( Process process );

  boolean removeAllEdges( Collection<? extends ProcessEdge> processEdges );

  Set<ProcessEdge> removeAllEdges( Process lhs, Process rhs );

  boolean removeAllVertices( Collection<? extends Process> processes );

  ProcessEdge removeEdge( Process lhs, Process rhs );

  boolean removeEdge( ProcessEdge processEdge );

  boolean removeVertex( Process process );

  Set<Process> vertexSet();

  Process getEdgeSource( ProcessEdge processEdge );

  Process getEdgeTarget( ProcessEdge processEdge );

  double getEdgeWeight( ProcessEdge processEdge );

  int inDegreeOf( Process process );

  Set<ProcessEdge> incomingEdgesOf( Process process );

  int outDegreeOf( Process process );

  Set<ProcessEdge> outgoingEdgesOf( Process process );

  Set<FlowElement> getSourceElements();

  Set<FlowElement> getSinkElements();

  Set<Tap> getSourceTaps();

  Map<String, Tap> getSourceTapsMap();

  Set<Tap> getSinkTaps();

  Map<String, Tap> getSinkTapsMap();

  Map<String, Tap> getTrapsMap();

  Iterator<Process> getTopologicalIterator();

  Iterator<Process> getOrdinalTopologicalIterator();

  Iterator<Process> getOrderedTopologicalIterator( Comparator<Process> comparator );

  List<ElementGraph> getElementGraphs( FlowElement flowElement );

  List<Process> getElementProcesses( FlowElement flowElement );

  List<ElementGraph> getElementGraphs( Scope scope );

  List<Process> getElementProcesses( Scope scope );

  List<Process> getElementSourceProcesses( FlowElement flowElement );

  List<Process> getElementSinkProcesses( FlowElement flowElement );

  Set<FlowElement> getAllSourceElements();

  Set<FlowElement> getAllSinkElements();

  EnumMultiMap<FlowElement> getAnnotations();

  Set<FlowElement> getDuplicatedElements( ElementGraph elementGraph );

  Set<ElementGraph> getIdentityElementGraphs();

  Set<Process> getIdentityProcesses();

  void writeDOT( String filename );

  void writeDOTNested( String filename, ElementGraph graph );
  }