/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import cascading.flow.planner.PlatformInfo;
import cascading.management.UnitOfWork;
import cascading.stats.FlowStats;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * A Flow is a logical unit of work declared by an assembly of {@link cascading.pipe.Pipe} instances connected to source
 * and sink {@link Tap} instances.
 * <p/>
 * A Flow is then executed to push the incoming source data through the assembly into one or more sinks.
 * <p/>
 * A Flow sub-class instance may not be instantiated directly in most cases, see sub-classes of {@link FlowConnector} class
 * for supported platforms.
 * <p/>
 * Note that {@link cascading.pipe.Pipe} assemblies can be reused in multiple Flow instances. They maintain
 * no state regarding the Flow execution. Subsequently, {@link cascading.pipe.Pipe} assemblies can be given
 * parameters through its calling Flow so they can be built in a generic fashion.
 * <p/>
 * When a Flow is created, an optimized internal representation is created that is then executed
 * on the underlying execution platform. This is typically done by creating one or more {@link FlowStep} instances.
 * </p>
 * Flows are submitted in order of dependency when used with a {@link cascading.cascade.Cascade}. If two or more steps do not share the
 * same dependencies and all can be scheduled simultaneously, the {@link #getSubmitPriority()} value determines
 * the order in which all steps will be submitted for execution. The default submit priority is 5.
 * <p/>
 * Use the {@link FlowListener} to receive any events on the life-cycle of the Flow as it executes. Any
 * {@link Tap} instances owned by the Flow also implementing FlowListener will automatically be added to the
 * set of listeners.
 *
 * @see FlowListener
 * @see cascading.flow.FlowConnector
 */
public interface Flow<Config> extends UnitOfWork<FlowStats>
  {
  String CASCADING_FLOW_ID = "cascading.flow.id";

  /**
   * Method getName returns the name of this Flow object.
   *
   * @return the name (type String) of this Flow object.
   */
  @Override
  String getName();

  /**
   * Method prepare is used by a {@link cascading.cascade.Cascade} to notify the given Flow it should initialize or clear any resources
   * necessary for {@link #start()} to be called successfully.
   * <p/>
   * Specifically, this implementation calls {@link BaseFlow#deleteSinksIfNotUpdate()} && {@link BaseFlow#deleteTrapsIfNotUpdate()}.
   *
   * @throws java.io.IOException when
   */
  @Override
  void prepare();

  /**
   * Method start begins the execution of this Flow instance. It will return immediately. Use the method {@link #complete()}
   * to block until this Flow completes.
   */
  @Override
  void start();

  /** Method stop stops all running jobs, killing any currently executing. */
  @Override
  void stop();

  /** Method complete starts the current Flow instance if it has not be previously started, then block until completion. */
  @Override
  void complete();

  @Override
  void cleanup();

  /**
   * Method getConfig returns the internal configuration object.
   * <p/>
   * Any changes to this object will not be reflected in child steps. See {@link cascading.flow.FlowConnector} for setting
   * default properties visible to children. Or see {@link cascading.flow.FlowStepStrategy} for setting properties on
   * individual steps before they are executed.
   *
   * @return the default configuration of this Flow
   */
  Config getConfig();

  /**
   * Method getConfigCopy returns a copy of the internal configuration object. This object can be safely
   * modified.
   *
   * @return a copy of the default configuration of this Flow
   */
  Config getConfigCopy();

  /**
   * Method getConfiAsProperties converts the internal configuration object into a {@link java.util.Map} of
   * key value pairs.
   *
   * @return a Map of key/value pairs
   */
  Map<Object, Object> getConfigAsProperties();

  String getProperty( String key );

  /**
   * Method getID returns the ID of this Flow object.
   * <p/>
   * The ID value is a long HEX String used to identify this instance globally. Subsequent Flow
   * instances created with identical parameters will not return the same ID.
   *
   * @return the ID (type String) of this Flow object.
   */
  @Override
  String getID();

  @Override
  String getTags();

  /**
   * Method getSubmitPriority returns the submitPriority of this Flow object.
   * <p/>
   * 10 is lowest, 1 is the highest, 5 is the default.
   *
   * @return the submitPriority (type int) of this FlowStep object.
   */
  int getSubmitPriority();

  /**
   * Method setSubmitPriority sets the submitPriority of this Flow object.
   * <p/>
   * 10 is lowest, 1 is the highest, 5 is the default.
   *
   * @param submitPriority the submitPriority of this FlowStep object.
   */
  void setSubmitPriority( int submitPriority );

  FlowProcess<Config> getFlowProcess();

  /**
   * Method getFlowStats returns the flowStats of this Flow object.
   *
   * @return the flowStats (type FlowStats) of this Flow object.
   */
  FlowStats getFlowStats();

  /**
   * Method hasListeners returns true if {@link FlowListener} instances have been registered.
   *
   * @return boolean
   */
  boolean hasListeners();

  /**
   * Method addListener registers the given flowListener with this instance.
   *
   * @param flowListener of type FlowListener
   */
  void addListener( FlowListener flowListener );

  /**
   * Method removeListener removes the given flowListener from this instance.
   *
   * @param flowListener of type FlowListener
   * @return true if the listener was removed
   */
  boolean removeListener( FlowListener flowListener );

  /**
   * Method getSources returns the sources of this Flow object.
   *
   * @return the sources (type Map) of this Flow object.
   */
  Map<String, Tap> getSources();

  List<String> getSourceNames();

  Tap getSource( String name );

  /**
   * Method getSourcesCollection returns a {@link Collection} of source {@link Tap}s for this Flow object.
   *
   * @return the sourcesCollection (type Collection<Tap>) of this Flow object.
   */
  Collection<Tap> getSourcesCollection();

  /**
   * Method getSinks returns the sinks of this Flow object.
   *
   * @return the sinks (type Map) of this Flow object.
   */
  Map<String, Tap> getSinks();

  List<String> getSinkNames();

  Tap getSink( String name );

  /**
   * Method getSinksCollection returns a {@link Collection} of sink {@link Tap}s for this Flow object.
   *
   * @return the sinkCollection (type Collection<Tap>) of this Flow object.
   */
  Collection<Tap> getSinksCollection();

  /**
   * Method getSink returns the first sink of this Flow object.
   *
   * @return the sink (type Tap) of this Flow object.
   */
  Tap getSink();

  /**
   * Method getTraps returns the traps of this Flow object.
   *
   * @return the traps (type Map<String, Tap>) of this Flow object.
   */
  Map<String, Tap> getTraps();

  List<String> getTrapNames();

  /**
   * Method getTrapsCollection returns a {@link Collection} of trap {@link Tap}s for this Flow object.
   *
   * @return the trapsCollection (type Collection<Tap>) of this Flow object.
   */
  Collection<Tap> getTrapsCollection();

  /**
   * Method getCheckpoints returns the checkpoint taps of this Flow object.
   *
   * @return the traps (type Map<String, Tap>) of this Flow object.
   */
  Map<String, Tap> getCheckpoints();

  List<String> getCheckpointNames();

  /**
   * Method getCheckpointsCollection returns a {@link Collection} of checkpoint {@link Tap}s for this Flow object.
   *
   * @return the trapsCollection (type Collection<Tap>) of this Flow object.
   */
  Collection<Tap> getCheckpointsCollection();


  /**
   * Method getFlowSkipStrategy returns the current {@link cascading.flow.FlowSkipStrategy} used by this Flow.
   *
   * @return FlowSkipStrategy
   */
  FlowSkipStrategy getFlowSkipStrategy();

  /**
   * Method setFlowSkipStrategy sets a new {@link cascading.flow.FlowSkipStrategy}, the current strategy is returned.
   * <p/>
   * FlowSkipStrategy instances define when a Flow instance should be skipped. The default strategy is {@link FlowSkipIfSinkNotStale}.
   * An alternative strategy would be {@link cascading.flow.FlowSkipIfSinkExists}.
   * <p/>
   * A FlowSkipStrategy will not be consulted when executing a Flow directly through {@link #start()} or {@link #complete()}. Only
   * when the Flow is executed through a {@link cascading.cascade.Cascade} instance.
   *
   * @param flowSkipStrategy of type FlowSkipStrategy
   * @return FlowSkipStrategy
   */
  FlowSkipStrategy setFlowSkipStrategy( FlowSkipStrategy flowSkipStrategy );

  /**
   * Method isSkipFlow returns true if the parent {@link cascading.cascade.Cascade} should skip this Flow instance. True is returned
   * if the current {@link cascading.flow.FlowSkipStrategy} returns true.
   *
   * @return the skipFlow (type boolean) of this Flow object.
   * @throws IOException when
   */
  boolean isSkipFlow() throws IOException;

  /**
   * Method areSinksStale returns true if any of the sinks referenced are out of date in relation to the sources. Or
   * if any sink method {@link cascading.tap.Tap#isReplace()} returns true.
   *
   * @return boolean
   * @throws java.io.IOException when
   */
  boolean areSinksStale() throws IOException;

  /**
   * Method areSourcesNewer returns true if any source is newer than the given sinkModified date value.
   *
   * @param sinkModified of type long
   * @return boolean
   * @throws java.io.IOException when
   */
  boolean areSourcesNewer( long sinkModified ) throws IOException;

  /**
   * Method getSinkModified returns the youngest modified date of any sink {@link cascading.tap.Tap} managed by this Flow instance.
   * <p/>
   * If zero (0) is returned, at least one of the sink resources does not exist. If minus one (-1) is returned,
   * atleast one of the sinks are marked for delete ({@link cascading.tap.Tap#isReplace() returns true}).
   *
   * @return the sinkModified (type long) of this Flow object.
   * @throws java.io.IOException when
   */
  long getSinkModified() throws IOException;

  /**
   * Returns the current {@link FlowStepStrategy} instance.
   *
   * @return FlowStepStrategy
   */
  FlowStepStrategy getFlowStepStrategy();

  /**
   * Sets a default {@link FlowStepStrategy} instance.
   * <p/>
   * Use a FlowStepStrategy to change {@link cascading.flow.FlowStep} configuration properties
   * before the properties are submitted to the underlying platform for the step
   * unit of work.
   *
   * @param flowStepStrategy The FlowStepStrategy to use.
   */
  void setFlowStepStrategy( FlowStepStrategy flowStepStrategy );

  /**
   * Method getFlowSteps returns the flowSteps of this Flow object. They will be in topological order.
   *
   * @return the steps (type List<FlowStep>) of this Flow object.
   */
  List<FlowStep<Config>> getFlowSteps();

  /**
   * Method openSource opens the first source Tap.
   *
   * @return TupleIterator
   * @throws IOException when
   */
  TupleEntryIterator openSource() throws IOException;

  /**
   * Method openSource opens the named source Tap.
   *
   * @param name of type String
   * @return TupleIterator
   * @throws IOException when
   */
  TupleEntryIterator openSource( String name ) throws IOException;

  /**
   * Method openSink opens the first sink Tap.
   *
   * @return TupleIterator
   * @throws IOException when
   */
  TupleEntryIterator openSink() throws IOException;

  /**
   * Method openSink opens the named sink Tap.
   *
   * @param name of type String
   * @return TupleIterator
   * @throws IOException when
   */
  TupleEntryIterator openSink( String name ) throws IOException;

  /**
   * Method openTrap opens the first trap Tap.
   *
   * @return TupleIterator
   * @throws IOException when
   */
  TupleEntryIterator openTrap() throws IOException;

  /**
   * Method openTrap opens the named trap Tap.
   *
   * @param name of type String
   * @return TupleIterator
   * @throws IOException when
   */
  TupleEntryIterator openTrap( String name ) throws IOException;

  /**
   * Method resourceExists returns true if the resource represented by the given Tap instance exists.
   *
   * @param tap of type Tap
   * @return boolean
   * @throws IOException when
   */
  boolean resourceExists( Tap tap ) throws IOException;

  /**
   * Method openTapForRead return a {@link cascading.tuple.TupleEntryIterator} for the given Tap instance.
   * <p/>
   * Note the returned iterator will return the same instance of {@link cascading.tuple.TupleEntry} on every call,
   * thus a copy must be made of either the TupleEntry or the underlying {@code Tuple} instance if they are to be
   * stored in a Collection.
   *
   * @param tap of type Tap
   * @return TupleIterator
   * @throws IOException when there is an error opening the resource
   */
  TupleEntryIterator openTapForRead( Tap tap ) throws IOException;

  /**
   * Method openTapForWrite returns a (@link TupleCollector} for the given Tap instance.
   *
   * @param tap of type Tap
   * @return TupleCollector
   * @throws IOException when there is an error opening the resource
   */
  TupleEntryCollector openTapForWrite( Tap tap ) throws IOException;

  /**
   * Method writeDOT writes this Flow instance to the given filename as a DOT file for import into a graphics package.
   *
   * @param filename of type String
   */
  void writeDOT( String filename );

  /**
   * Method writeStepsDOT writes this Flow step graph to the given filename as a DOT file for import into a graphics package.
   *
   * @param filename of type String
   */
  void writeStepsDOT( String filename );

  String getCascadeID();

  String getRunID();

  PlatformInfo getPlatformInfo();

  /**
   * Method jobsAreLocal returns true if all jobs are executed in-process as a single map and reduce task.
   *
   * @return boolean
   */
  boolean stepsAreLocal();

  /**
   * Method isStopJobsOnExit returns the stopJobsOnExit of this Flow object. Defaults to {@code true}.
   *
   * @return the stopJobsOnExit (type boolean) of this Flow object.
   */
  boolean isStopJobsOnExit();
  }