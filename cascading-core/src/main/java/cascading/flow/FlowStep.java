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

package cascading.flow;

import java.util.Map;
import java.util.Set;

import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.process.ProcessModel;
import cascading.pipe.Group;
import cascading.stats.FlowStepStats;
import cascading.tap.Tap;

/**
 * Class FlowStep is an internal representation of a given "job" possibly to be executed on a remote cluster. During
 * planning, pipe assemblies are broken down into "steps" and encapsulated in this class.
 * <p/>
 * FlowSteps are submitted in order of dependency. If two or more steps do not share the same dependencies and all
 * can be scheduled simultaneously, the {@link #getSubmitPriority()} value determines the order in which
 * all steps will be submitted for execution. The default submit priority is 5.
 */
public interface FlowStep<Config> extends ProcessModel
  {
  String CASCADING_FLOW_STEP_ID = "cascading.flow.step.id";

  /**
   * Method getId returns the id of this FlowStep object.
   *
   * @return the id (type int) of this FlowStep object.
   */
  String getID();

  int getOrdinal();

  /**
   * Method getName returns the name of this FlowStep object.
   *
   * @return the name (type String) of this FlowStep object.
   */
  String getName();

  /**
   * Returns an immutable map of properties giving more details about the FlowStep object.
   * <p/>
   * FlowStep descriptions provide meta-data to monitoring systems describing the workload a given FlowStep represents.
   * For known description types, see {@link FlowStepDescriptors}.
   *
   * @return Map<String,String>
   */
  Map<String, String> getFlowStepDescriptor();

  Flow<Config> getFlow();

  String getFlowID();

  /**
   * Method getParentFlowName returns the parentFlowName of this FlowStep object.
   *
   * @return the parentFlowName (type Flow) of this FlowStep object.
   */
  String getFlowName();

  /**
   * Method getConfig returns the current initialized configuration.
   * <p/>
   * The returned configuration is mutable and may be changed prior to this step being started
   * or submitted.
   *
   * @return the current initialized configuration
   */
  Config getConfig();

  /**
   * Method getConfigAsProperties converts the internal configuration object into a {@link java.util.Map} of
   * key value pairs.
   *
   * @return a Map of key/value pairs, may return an empty collection if unsupported
   */
  Map<Object, Object> getConfigAsProperties();

  /**
   * Method getStepDisplayName returns the stepDisplayName of this FlowStep object.
   *
   * @return the stepName (type String) of this FlowStep object.
   */
  String getStepDisplayName();

  /**
   * Method getSubmitPriority returns the submitPriority of this FlowStep object.
   * <p/>
   * 10 is lowest, 1 is the highest, 5 is the default.
   *
   * @return the submitPriority (type int) of this FlowStep object.
   */
  int getSubmitPriority();

  /**
   * Method setSubmitPriority sets the submitPriority of this FlowStep object.
   * <p/>
   * 10 is lowest, 1 is the highest, 5 is the default.
   *
   * @param submitPriority the submitPriority of this FlowStep object.
   */
  void setSubmitPriority( int submitPriority );

  FlowNodeGraph getFlowNodeGraph();

  int getNumFlowNodes();

  Group getGroup();

  Tap getSink();

  Set<String> getSourceName( Tap source );

  Set<String> getSinkName( Tap sink );

  Tap getSourceWith( String identifier );

  Tap getSinkWith( String identifier );

  Set<Tap> getTraps();

  Tap getTrap( String name );

  /**
   * Returns true if this FlowStep contains a pipe/branch with the given name.
   *
   * @param pipeName the name of the Pipe
   * @return a boolean
   */
  boolean containsPipeNamed( String pipeName );

  FlowStepStats getFlowStepStats();

  /**
   * Method hasListeners returns true if {@link FlowStepListener} instances have been registered.
   *
   * @return boolean
   */
  boolean hasListeners();

  /**
   * Method addListener registers the given {@link FlowStepListener} with this instance.
   *
   * @param flowStepListener of type flowStepListener
   */
  void addListener( FlowStepListener flowStepListener );

  /**
   * Method removeListener removes the given flowStepListener from this instance.
   *
   * @param flowStepListener of type FlowStepListener
   * @return true if the listener was removed
   */
  boolean removeListener( FlowStepListener flowStepListener );
  }