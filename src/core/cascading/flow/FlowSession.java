/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import cascading.management.CascadingServices;

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

  /** Field NULL is a noop implementation of FlowSession. */
  public static final FlowSession NULL = new FlowSession();
  private Map<Object, Object> properties;
  private String id;

  protected CascadingServices cascadingServices;

  public FlowSession()
    {
    }

  public FlowSession( CascadingServices cascadingServices )
    {
    this.cascadingServices = cascadingServices;
    }

  public FlowSession( CascadingServices cascadingServices, FlowProcess currentProcess )
    {
    this.cascadingServices = cascadingServices;
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

  public CascadingServices getCascadingServices()
    {
    return cascadingServices;
    }

  public Map<Object, Object> getProperties()
    {
    return properties;
    }

  public String getID()
    {
    return id;
    }
  }
