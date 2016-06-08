/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.ProcessModel;
import cascading.stats.FlowNodeStats;
import cascading.tap.Tap;

/**
 * Class FlowNode represents the smallest parallelizable unit of work. It is a child to a
 * {@link cascading.flow.FlowStep} and may have many siblings within the FlowStep.
 * <p/>
 * A FlowNode is commonly executed as one or more slices, where a slice is a JVM executing against a portion
 * of data.
 * <p/>
 * Most slices within a FlowNode are identical, except for the sub-set of data they will be processing against.
 * <p/>
 * But on some platforms, like MapReduce, a slice is executing a single flow pipeline. Thus a FlowNode may consist of
 * some set of pipelines (or pipeline graph). One pipeline per 'streamed' input source Tap.
 * <p/>
 * In a MapReduce model (like Apache Hadoop MapReduce) a FlowNode can by the Map or Reduce side of a job (where a job
 * is a FlowStep).
 * <p/>
 * In a DAG model (like Apache Tez), a FlowNode is a 'vertex', and the 'DAG' is a FlowStep.
 */
public interface FlowNode extends ProcessModel
  {
  String CASCADING_FLOW_NODE = "cascading.flow.node";

  String getID();

  /**
   * Returns an immutable map of properties giving more details about the FlowNode object.
   * <p/>
   * FlowNode descriptions provide meta-data to monitoring systems describing the workload a given FlowNode represents.
   * For known description types, see {@link FlowNodeDescriptors}.
   *
   * @return Map<String,String>
   */
  Map<String, String> getFlowNodeDescriptor();

  FlowNodeStats getFlowNodeStats();

  FlowStep getFlowStep();

  Collection<? extends FlowElement> getFlowElementsFor( Enum annotation );

  Set<? extends FlowElement> getSourceElements( Enum annotation );

  Set<? extends FlowElement> getSinkElements( Enum annotation );

  Set<String> getSourceElementNames();

  Set<String> getSinkElementNames();

  Set<String> getSourceTapNames( Tap flowElement );

  Set<String> getSinkTapNames( Tap flowElement );

  Tap getTrap( String branchName );

  Collection<? extends Tap> getTraps();

  Collection<? extends Scope> getPreviousScopes( FlowElement flowElement );

  Collection<? extends Scope> getNextScopes( FlowElement flowElement );

  List<? extends ElementGraph> getPipelineGraphs();

  ElementGraph getPipelineGraphFor( FlowElement streamedSource );
  }