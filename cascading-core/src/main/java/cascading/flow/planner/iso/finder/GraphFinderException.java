/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner.iso.finder;

import cascading.CascadingException;
import cascading.util.Util;
import org.jgrapht.DirectedGraph;

/**
 *
 */
public class GraphFinderException extends CascadingException
  {
  private DirectedGraph graph;

  public GraphFinderException()
    {
    }

  public GraphFinderException( String message )
    {
    super( message );
    }

  public GraphFinderException( String message, Throwable throwable )
    {
    super( message, throwable );
    }

  public GraphFinderException( Throwable throwable )
    {
    super( throwable );
    }

  public GraphFinderException( String message, DirectedGraph graph )
    {
    this( message );
    this.graph = graph;
    }

  public DirectedGraph getGraph()
    {
    return graph;
    }

  public void writeDOT( String filename )
    {
    Util.invokeInstanceMethod( graph, "writeDOT", new Object[]{filename}, new Class[]{String.class} );
    }
  }
