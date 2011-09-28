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

import cascading.CascadingException;

/** FlowException instances are thrown on errors when executing a Flow. */
public class FlowException extends CascadingException
  {
  /** Field flowName */
  String flowName;

  /** Constructor FlowException creates a new FlowException instance. */
  public FlowException()
    {
    }

  /**
   * Constructor FlowException creates a new FlowException instance.
   *
   * @param message of type String
   */
  public FlowException( String message )
    {
    super( message );
    }

  /**
   * Constructor FlowException creates a new FlowException instance.
   *
   * @param message   of type String
   * @param throwable of type Throwable
   */
  public FlowException( String message, Throwable throwable )
    {
    super( message, throwable );
    }

  /**
   * Constructor FlowException creates a new FlowException instance.
   *
   * @param flowName  of type String
   * @param message   of type String
   * @param throwable of type Throwable
   */
  public FlowException( String flowName, String message, Throwable throwable )
    {
    super( message, throwable );
    this.flowName = flowName;
    }

  /**
   * Constructor FlowException creates a new FlowException instance.
   *
   * @param throwable of type Throwable
   */
  public FlowException( Throwable throwable )
    {
    super( throwable );
    }

  /**
   * Method getFlowName returns the name of the parent {@link Flow} instance.
   *
   * @return the flowName (type String) of this FlowException object.
   */
  public String getFlowName()
    {
    return flowName;
    }

  void setFlowName( String flowName )
    {
    this.flowName = flowName;
    }
  }
