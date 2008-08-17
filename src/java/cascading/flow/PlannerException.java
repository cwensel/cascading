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

import org.jgrapht.graph.SimpleDirectedGraph;

/**
 * Class PlannerException is thrown when a job planner fails.
 * <p/>
 * For debugging purposes, the PlannerException holds a copy of the internal job representation which can be
 * written out to disk and visualized with tools that support the
 * <a href="http://en.wikipedia.org/wiki/DOT_language">DOT file format</a> using the {@link #writeDOT(String)}
 * method.
 */
public class PlannerException extends FlowException
  {
  /** Field pipeGraph */
  SimpleDirectedGraph<FlowElement, Scope> pipeGraph;

  /** Constructor PlannerException creates a new PlannerException instance. */
  public PlannerException()
    {
    }

  /**
   * Constructor PlannerException creates a new PlannerException instance.
   *
   * @param string of type String
   */
  public PlannerException( String string )
    {
    super( string );
    }

  /**
   * Constructor PlannerException creates a new PlannerException instance.
   *
   * @param string    of type String
   * @param throwable of type Throwable
   */
  public PlannerException( String string, Throwable throwable )
    {
    super( string, throwable );
    }

  /**
   * Constructor PlannerException creates a new PlannerException instance.
   *
   * @param throwable of type Throwable
   */
  public PlannerException( Throwable throwable )
    {
    super( throwable );
    }

  /**
   * Constructor PlannerException creates a new PlannerException instance.
   *
   * @param string    of type String
   * @param throwable of type Throwable
   * @param pipeGraph of type SimpleDirectedGraph<FlowElement, Scope>
   */
  public PlannerException( String string, Throwable throwable, SimpleDirectedGraph<FlowElement, Scope> pipeGraph )
    {
    super( string, throwable );
    this.pipeGraph = pipeGraph;
    }

  /**
   * Method getPipeGraph returns the pipeGraph of this PlannerException object.
   *
   * @return the pipeGraph (type SimpleDirectedGraph<FlowElement, Scope>) of this PlannerException object.
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
