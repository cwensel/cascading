/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.cascade;

import java.util.Collection;
import java.util.List;

import cascading.flow.Flow;
import cascading.flow.FlowSkipStrategy;
import cascading.management.UnitOfWork;
import cascading.stats.CascadeStats;
import cascading.tap.Tap;

/**
 * A Cascade is an assembly of {@link cascading.flow.Flow} instances that share or depend on equivalent {@link Tap} instances and are executed as
 * a single group. The most common case is where one Flow instance depends on a Tap created by a second Flow instance. This
 * dependency chain can continue as practical.
 * <p/>
 * Note Flow instances that have no shared dependencies will be executed in parallel.
 * <p/>
 * Additionally, a Cascade allows for incremental builds of complex data processing processes. If a given source {@link Tap} is newer than
 * a subsequent sink {@link Tap} in the assembly, the connecting {@link cascading.flow.Flow}(s) will be executed
 * when the Cascade executed. If all the targets (sinks) are up to date, the Cascade exits immediately and does nothing.
 * <p/>
 * The concept of 'stale' is pluggable, see the {@link cascading.flow.FlowSkipStrategy} class.
 * <p/>
 * When a Cascade starts up, if first verifies which Flow instances have stale sinks, if the sinks are not stale, the
 * method {@link cascading.flow.BaseFlow#deleteSinksIfNotUpdate()} is called. Before appends/updates were supported (logically)
 * the Cascade deleted all the sinks in a Flow.
 * <p/>
 * The new consequence of this is if the Cascade fails, but does complete a Flow that appended or updated data, re-running
 * the Cascade (and the successful append/update Flow) will re-update data to the source. Some systems may be idempotent and
 * may not have any side-effects. So plan accordingly.
 * <p/>
 * Use the {@link CascadeListener} to receive any events on the life-cycle of the Cascade as it executes. Any
 * {@link Tap} instances owned by managed Flows also implementing CascadeListener will automatically be added to the
 * set of listeners.
 *
 * @see CascadeListener
 * @see cascading.flow.Flow
 * @see cascading.flow.FlowSkipStrategy
 */
public interface Cascade extends UnitOfWork<CascadeStats>
  {
  boolean hasListeners();

  void addListener( CascadeListener cascadeListener );

  boolean removeListener( CascadeListener flowListener );

  /**
   * Method getCascadeStats returns the cascadeStats of this Cascade object.
   *
   * @return the cascadeStats (type CascadeStats) of this Cascade object.
   */
  CascadeStats getCascadeStats();

  /**
   * Method getFlows returns the flows managed by this Cascade object. The returned {@link cascading.flow.Flow} instances
   * will be in topological order.
   *
   * @return the flows (type Collection<Flow>) of this Cascade object.
   */
  List<Flow> getFlows();

  /**
   * Method findFlows returns a List of flows whose names match the given regex pattern.
   *
   * @param regex of type String
   * @return List<Flow>
   */
  List<Flow> findFlows( String regex );

  /**
   * Method getHeadFlows returns all Flow instances that are at the "head" of the flow graph.
   * <p/>
   * That is, they are the first to execute and have no Tap source dependencies with Flow instances in the this Cascade
   * instance.
   *
   * @return Collection<Flow>
   */
  Collection<Flow> getHeadFlows();

  /**
   * Method getTailFlows returns all Flow instances that are at the "tail" of the flow graph.
   * <p/>
   * That is, they are the last to execute and have no Tap sink dependencies with Flow instances in the this Cascade
   * instance.
   *
   * @return Collection<Flow>
   */
  Collection<Flow> getTailFlows();

  /**
   * Method getIntermediateFlows returns all Flow instances that are neither at the "tail" or "tail" of the flow graph.
   *
   * @return Collection<Flow>
   */
  Collection<Flow> getIntermediateFlows();

  /**
   * Method getSourceTaps returns all source Tap instances in this Cascade instance.
   * <p/>
   * That is, none of returned Tap instances are the sinks of other Flow instances in this Cascade.
   * <p/>
   * All {@link cascading.tap.CompositeTap} instances are unwound if addressed directly by a managed Flow instance.
   *
   * @return Collection<Tap>
   */
  Collection<Tap> getSourceTaps();

  /**
   * Method getSinkTaps returns all sink Tap instances in this Cascade instance.
   * <p/>
   * That is, none of returned Tap instances are the sources of other Flow instances in this Cascade.
   * <p/>
   * All {@link cascading.tap.CompositeTap} instances are unwound if addressed directly by a managed Flow instance.
   * <p/>
   * This method will return checkpoint Taps managed by Flow instances if not used as a source by other Flow instances.
   *
   * @return Collection<Tap>
   */
  Collection<Tap> getSinkTaps();

  /**
   * Method getCheckpointTaps returns all checkpoint Tap instances from all the Flow instances in this Cascade instance.
   *
   * @return Collection<Tap>
   */
  Collection<Tap> getCheckpointsTaps();

  /**
   * Method getIntermediateTaps returns all Tap instances that are neither at the source or sink of the flow graph.
   * <p/>
   * This method does consider checkpoint Taps managed by Flow instances in this Cascade instance.
   *
   * @return Collection<Flow>
   */
  Collection<Tap> getIntermediateTaps();

  /**
   * Method getAllTaps returns all source, sink, and checkpoint Tap instances associated with the managed
   * Flow instances in this Cascade instance.
   *
   * @return Collection<Tap>
   */
  Collection<Tap> getAllTaps();

  /**
   * Method getSuccessorFlows returns a Collection of all the Flow instances that will be
   * executed after the given Flow instance.
   *
   * @param flow of type Flow
   * @return Collection<Flow>
   */
  Collection<Flow> getSuccessorFlows( Flow flow );

  /**
   * Method getPredecessorFlows returns a Collection of all the Flow instances that will be
   * executed before the given Flow instance.
   *
   * @param flow of type Flow
   * @return Collection<Flow>
   */
  Collection<Flow> getPredecessorFlows( Flow flow );

  /**
   * Method findFlowsSourcingFrom returns all Flow instances that reads from a source with the given identifier.
   *
   * @param identifier of type String
   * @return Collection<Flow>
   */
  Collection<Flow> findFlowsSourcingFrom( String identifier );

  /**
   * Method findFlowsSinkingTo returns all Flow instances that writes to a sink with the given identifier.
   *
   * @param identifier of type String
   * @return Collection<Flow>
   */
  Collection<Flow> findFlowsSinkingTo( String identifier );

  /**
   * Method getFlowSkipStrategy returns the current {@link cascading.flow.FlowSkipStrategy} used by this Flow.
   *
   * @return FlowSkipStrategy
   */
  FlowSkipStrategy getFlowSkipStrategy();

  /**
   * Method setFlowSkipStrategy sets a new {@link cascading.flow.FlowSkipStrategy}, the current strategy, if any, is returned.
   * If a strategy is given, it will be used as the strategy for all {@link cascading.flow.BaseFlow} instances managed by this Cascade instance.
   * To revert back to consulting the strategies associated with each Flow instance, re-set this value to {@code null}, its
   * default value.
   * <p/>
   * FlowSkipStrategy instances define when a Flow instance should be skipped. The default strategy is {@link cascading.flow.FlowSkipIfSinkNotStale}
   * and is inherited from the Flow instance in question. An alternative strategy would be {@link cascading.flow.FlowSkipIfSinkExists}.
   * <p/>
   * A FlowSkipStrategy will not be consulted when executing a Flow directly through {@link #start()}
   *
   * @param flowSkipStrategy of type FlowSkipStrategy
   * @return FlowSkipStrategy
   */
  FlowSkipStrategy setFlowSkipStrategy( FlowSkipStrategy flowSkipStrategy );

  /**
   * Method start begins the current Cascade process. It returns immediately. See method {@link #complete()} to block
   * until the Cascade completes.
   */
  void start();

  /**
   * Method complete begins the current Cascade process if method {@link #start()} was not previously called. This method
   * blocks until the process completes.
   *
   * @throws RuntimeException wrapping any exception thrown internally.
   */
  void complete();

  void stop();

  /**
   * Method writeDOT writes this element graph to a DOT file for easy visualization and debugging.
   *
   * @param filename of type String
   */
  void writeDOT( String filename );
  }