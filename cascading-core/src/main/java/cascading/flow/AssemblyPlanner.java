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

import java.util.List;

import cascading.pipe.Pipe;

/**
 * Interface AssemblyPlanner is used to allow for lazy evaluation of a pipe assembly during planning of a {@link Flow}.
 * <p/>
 * This allows for new languages or frameworks that may require additional meta-data from the
 * underlying platform or environment. Specifically field names and type information from incoming source
 * and outgoing sink {@link cascading.tap.Tap}s.
 * <p/>
 * AssemblyPlanner implementations are handed to a {@link cascading.flow.planner.FlowPlanner}
 * instance in the order they should be evaluated.
 * Every instance has the opportunity to replace any prior tails with new branches and paths.
 * <p/>
 * Every instance of AssemblyPlanner evaluated is given the current {@link FlowDef} used on the current
 * {@link FlowConnector}, the current Flow instance (only initialized with source and sink Taps provided by the
 * FlowDef), and the tails provided by the FlowDef or those returned by prior AssemblyPlanner instances.
 * <p/>
 * An AssemblyPlanner cannot change or modify the Flow, or change out any Taps used as sources, sinks, traps, or
 * checkpoints.
 * <p/>
 * This is an experimental API and subject to change without notice.
 */
public interface AssemblyPlanner
  {
  interface Context
    {
    FlowDef getFlowDef();

    Flow getFlow();

    List<Pipe> getTails();
    }

  /**
   * Called when this AssemblyPlanner instance should return any additional tail Pipe instances for used
   * when completing the Flow plan.
   *
   * @param context parameter object of the Context
   * @return tail Pipe instances to replace the given tails
   */
  List<Pipe> resolveTails( Context context );
  }
