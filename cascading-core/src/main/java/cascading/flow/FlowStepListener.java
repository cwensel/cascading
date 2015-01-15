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

/** Interface FlowStepListener provides hooks for receiving events on various stages of a {@link FlowStep} execution. */
public interface FlowStepListener
  {
  /**
   * The onStarting event is fired when a  given {@link cascading.flow.FlowStep} job has been submitted i.e. to hadoop cluster
   *
   * @param flowStep
   */
  public void onStepStarting( FlowStep flowStep );

  /**
   * The onStepStopping event is fired when a given {@link cascading.flow.FlowStep} job is stopped
   *
   * @param flowStep
   */
  public void onStepStopping( FlowStep flowStep );

  /**
   * The onStepRunning event is fired when a given {@link cascading.flow.FlowStep} moves into the running state
   *
   * @param flowStep
   */
  public void onStepRunning( FlowStep flowStep );

  /**
   * The onStepCompleted event is fired when a flowStepJob completed its work
   *
   * @param flowStep
   */
  public void onStepCompleted( FlowStep flowStep );

  /**
   * The onStepThrowable event is fired if a given {@link cascading.flow.FlowStep} throws a Throwable type. This throwable is passed
   * as an argument to the event. This event method should return true if the given throwable was handled and should
   * not be rethrown from the {@link Flow#complete()} method.
   *
   * @param flowStep
   * @param throwable
   * @return returns true if this listener has handled the given throwable
   */
  public boolean onStepThrowable( FlowStep flowStep, Throwable throwable );
  }