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

import cascading.CascadingException;
import org.jgrapht.graph.SimpleDirectedGraph;

/** FlowException instances are thrown on errors when building a Flow or when executing one. */
public class FlowException extends CascadingException
  {
  /** Field pipeGraph */
  SimpleDirectedGraph<FlowElement, Scope> pipeGraph;

  /** Constructor FlowException creates a new FlowException instance. */
  public FlowException()
    {
    }

  /**
   * Constructor FlowException creates a new FlowException instance.
   *
   * @param string of type String
   */
  public FlowException( String string )
    {
    super( string );
    }

  /**
   * Constructor FlowException creates a new FlowException instance.
   *
   * @param string    of type String
   * @param throwable of type Throwable
   */
  public FlowException( String string, Throwable throwable )
    {
    super( string, throwable );
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
   * Constructor FlowException creates a new FlowException instance.
   *
   * @param string    of type String
   * @param throwable of type Throwable
   * @param pipeGraph of type SimpleDirectedGraph<FlowElement, Scope>
   */
  public FlowException( String string, Throwable throwable, SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    this( string, throwable );
    this.pipeGraph = pipeGraph;
    }

  /**
   * Method getPipeGraph returns the pipeGraph of this FlowException object.
   *
   * @return the pipeGraph (type SimpleDirectedGraph<FlowElement, Scope>) of this FlowException object.
   */
  public SimpleDirectedGraph<FlowElement, Scope> getPipeGraph()
    {
    return pipeGraph;
    }

  /**
   * Method writeDOT writes the failed Flow instance to the given filename as a DOT file for import into a graphics package.
   *
   * @param filename of type String
   */
  public void writeDOT( String filename )
    {
    if( pipeGraph == null )
      return;

    Flow.printElementGraph( filename, pipeGraph );
    }
  }
