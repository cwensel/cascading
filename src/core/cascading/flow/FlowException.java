/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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
