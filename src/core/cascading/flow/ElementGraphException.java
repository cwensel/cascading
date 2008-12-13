/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import cascading.pipe.Pipe;

/**
 * Class ElementGraphException is thrown during rendering of a pipe assembly to
 * the Cascading internal "graph" representation.
 */
public class ElementGraphException extends FlowException
  {
  /** Field flowElement  */
  private FlowElement flowElement;

  /** Constructor ElementGraphException creates a new ElementGraphException instance. */
  public ElementGraphException()
    {
    }

  /**
   * Constructor ElementGraphException creates a new ElementGraphException instance.
   *
   * @param flowElement of type FlowElement
   * @param message of type String
   */
  public ElementGraphException( FlowElement flowElement, String message )
    {
    super( message );
    this.flowElement = flowElement;
    }

  /**
   * Constructor ElementGraphException creates a new ElementGraphException instance.
   *
   * @param flowElement of type FlowElement
   * @param message of type String
   * @param throwable of type Throwable
   */
  public ElementGraphException( FlowElement flowElement, String message, Throwable throwable )
    {
    super( message, throwable );
    this.flowElement = flowElement;
    }

  /**
   * Constructor ElementGraphException creates a new ElementGraphException instance.
   *
   * @param string of type String
   */
  public ElementGraphException( String string )
    {
    super( string );
    }

  /**
   * Constructor ElementGraphException creates a new ElementGraphException instance.
   *
   * @param string    of type String
   * @param throwable of type Throwable
   */
  public ElementGraphException( String string, Throwable throwable )
    {
    super( string, throwable );
    }

  /**
   * Constructor ElementGraphException creates a new ElementGraphException instance.
   *
   * @param throwable of type Throwable
   */
  public ElementGraphException( Throwable throwable )
    {
    super( throwable );
    }

  /**
   * Method getFlowElement returns the flowElement of this ElementGraphException object.
   *
   * @return the flowElement (type FlowElement) of this ElementGraphException object.
   */
  public FlowElement getFlowElement()
    {
    return flowElement;
    }

  Pipe getPipe()
    {
    if(flowElement instanceof Pipe)
      return (Pipe) flowElement;

    return null;
    }
  }