/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow;

/** Interface FlowListener provides hooks for receiving events on various stages of a {@link Flow} execution. */
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
   * The onThrowable event is fired if any child {@link FlowStep} throws a Throwable type. This throwable is passed
   * as an argument to the event. This event method should return true if the given throwable was handled and should
   * not be rethrown from the {@link Flow#complete()} method.
   *
   * @param flow
   * @param throwable
   * @return returns true if this listener has handled the given throwable
   */
  boolean onThrowable( Flow flow, Throwable throwable );
  }