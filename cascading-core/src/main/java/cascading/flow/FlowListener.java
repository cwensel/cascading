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

/**
 * Interface FlowListener provides hooks for receiving events on various stages of a {@link Flow} execution.
 * <p/>
 * Any {@link RuntimeException} thrown from any of the listener methods will force the given {@code flow} to
 * stop by calling {@link Flow#stop()}.
 */
public interface FlowListener
  {
  /**
   * The onStarting event is fired when a Flow instance receives the start() message.
   *
   * @param flow
   */
  void onStarting( Flow flow );

  /**
   * The onStopping event is fired when a Flow instance receives the stop() message.
   *
   * @param flow
   */
  void onStopping( Flow flow );

  /**
   * The onCompleted event is fired when a Flow instance has completed all work whether if was success or failed. If
   * there was a thrown exception, onThrowable will be fired before this event.
   *
   * @param flow
   */
  void onCompleted( Flow flow );

  /**
   * The onThrowable event is fired if any child {@link cascading.flow.FlowStep} throws a Throwable type. This throwable is passed
   * as an argument to the event. This event method should return true if the given throwable was handled and should
   * not be rethrown from the {@link Flow#complete()} method.
   *
   * @param flow
   * @param throwable
   * @return returns true if this listener has handled the given throwable
   */
  boolean onThrowable( Flow flow, Throwable throwable );
  }