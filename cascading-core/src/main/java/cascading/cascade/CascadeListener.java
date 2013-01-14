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

package cascading.cascade;

/**
 * Interface CascadeListener provides hooks for receiving events on various stages of a {@link cascading.cascade.Cascade} execution.
 * <p/>
 * Any {@link RuntimeException} thrown from any of the listener methods will force the given {@code cascade} to
 * stop by calling {@link cascading.cascade.Cascade#stop()}.
 */
public interface CascadeListener
  {
  /**
   * The onStarting event is fired when a Cascade instance receives the start() message.
   *
   * @param cascade the current Cascade
   */
  void onStarting( Cascade cascade );

  /**
   * The onStopping event is fired when a Cascade instance receives the stop() message.
   *
   * @param cascade the current Cascade
   */
  void onStopping( Cascade cascade );

  /**
   * The onCompleted event is fired when a Cascade instance has completed all work whether if was success or failed. If
   * there was a thrown exception, onThrowable will be fired before this event.
   *
   * @param cascade the current Cascade
   */
  void onCompleted( Cascade cascade );

  /**
   * The onThrowable event is fired if any child {@link cascading.flow.Flow} throws a Throwable type. This throwable is passed
   * as an argument to the event. This event method should return true if the given throwable was handled and should
   * not be rethrown from the {@link cascading.cascade.Cascade#complete()} method.
   *
   * @param cascade   the current Cascade
   * @param throwable the current error
   * @return returns true if this listener has handled the given throwable
   */
  boolean onThrowable( Cascade cascade, Throwable throwable );
  }