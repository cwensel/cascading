/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

/**
 * FlowSession implementations provide a call-back interface into the current flow management system, if any.
 * <p/>
 * A FlowSession is effectively unique to the current {@link Flow}, where a {@link FlowProcess} is unique
 * to each underlying 'job'.
 *
 * @see FlowProcess
 */
public class FlowSession
  {
  /** Field currentProcess */
  private FlowProcess currentProcess;

  /** Field NULL is a noop implemenation of FlowSession. */
  public static final FlowSession NULL = new FlowSession();


  /** Constructor FlowSession creates a new FlowSession instance. */
  public FlowSession()
    {
    }

  /**
   * Constructor FlowSession creates a new FlowSession instance.
   *
   * @param currentProcess of type FlowProcess
   */
  public FlowSession( FlowProcess currentProcess )
    {
    this.currentProcess = currentProcess;
    }

  /**
   * Method setCurrentProcess sets the currentProcess of this FlowSession object.
   *
   * @param currentProcess the currentProcess of this FlowSession object.
   */
  public void setCurrentProcess( FlowProcess currentProcess )
    {
    this.currentProcess = currentProcess;
    }

  /**
   * Method getCurrentProcess returns the currentProcess of this FlowSession object.
   *
   * @return the currentProcess (type FlowProcess) of this FlowSession object.
   */
  public FlowProcess getCurrentProcess()
    {
    return currentProcess;
    }

  }
