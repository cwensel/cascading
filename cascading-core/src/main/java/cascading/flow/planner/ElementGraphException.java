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

package cascading.flow.planner;

import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.util.Util;

/**
 * Class ElementGraphException is thrown during rendering of a pipe assembly to
 * the Cascading internal "graph" representation.
 */
public class ElementGraphException extends FlowException
  {
  /** Field flowElement */
  private FlowElement flowElement;

  /** Constructor ElementGraphException creates a new ElementGraphException instance. */
  public ElementGraphException()
    {
    }

  public ElementGraphException( Pipe pipe, String message )
    {
    this( (FlowElement) pipe, Util.formatTrace( pipe, message ) );
    }

  public ElementGraphException( Tap tap, String message )
    {
    this( (FlowElement) tap, Util.formatTrace( tap, message ) );
    }

  /**
   * Constructor ElementGraphException creates a new ElementGraphException instance.
   *
   * @param flowElement of type FlowElement
   * @param message     of type String
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
   * @param message     of type String
   * @param throwable   of type Throwable
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
    if( flowElement instanceof Pipe )
      return (Pipe) flowElement;

    return null;
    }
  }