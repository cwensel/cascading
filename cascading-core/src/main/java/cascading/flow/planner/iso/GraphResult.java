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

package cascading.flow.planner.iso;

import java.io.File;

import cascading.flow.planner.graph.ElementGraph;

/**
 *
 */
public abstract class GraphResult<R extends ElementGraph>
  {
  public abstract ElementGraph getBeginGraph();

  public abstract R getEndGraph();

  public abstract void writeDOTs( String path );

  protected void writeEndGraph( String path, int count )
    {
    if( getEndGraph() != null )
      {
      String name = getEndGraph().getClass().getSimpleName();
      getEndGraph().writeDOT( new File( path, makeFileName( count, name, "end" ) ).toString() );
      }
    }

  protected int writeBeginGraph( String path, int count )
    {
    if( getBeginGraph() != null )
      {
      String name = getBeginGraph().getClass().getSimpleName();
      getBeginGraph().writeDOT( new File( path, makeFileName( count++, name, "begin" ) ).toString() );
      }

    return count;
    }

  protected String makeFileName( int ordinal, String name, String state )
    {
    return String.format( "%04d-%s-%s.dot", ordinal, name, state );
    }
  }