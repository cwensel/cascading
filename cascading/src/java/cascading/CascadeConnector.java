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

package cascading;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import cascading.flow.Flow;
import cascading.tap.Tap;
import cascading.util.Util;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.jgrapht.graph.SimpleDirectedGraph;

/** Class CascadeConnector is used to construct a new {@link Cascade} instance from a collection of {@link Flow} instance. */
public class CascadeConnector
  {

  /**
   * Given any number of {@link Flow} objects, it will connect them and return a new {@link Cascade} instance. The name
   * of the Cascade is derived from the given Flow instances.
   *
   * @param flows of type Flow
   * @return Cascade
   */
  public Cascade connect( Flow... flows )
    {
    return connect( null, flows );
    }

  /**
   * Given any number of {@link Flow} objects, it will connect them and return a new {@link Cascade} instance.
   *
   * @param name  of type String
   * @param flows of type Flow
   * @return Cascade
   */
  public Cascade connect( String name, Flow... flows )
    {
    name = name == null ? makeName( flows ) : name;

    SimpleDirectedGraph<Tap, Flow.FlowHolder> graph = new SimpleDirectedGraph<Tap, Flow.FlowHolder>( Flow.FlowHolder.class );

    makeGraph( graph, flows );
    Tap rootTap = getRoots( graph );

    return new Cascade( name, rootTap, graph );
    }

  private String makeName( Flow[] flows )
    {
    String[] names = new String[flows.length];

    for( int i = 0; i < flows.length; i++ )
      names[ i ] = flows[ i ].getName();

    return Util.join( names, "+" );
    }

  private Tap getRoots( SimpleDirectedGraph<Tap, Flow.FlowHolder> graph )
    {
    Tap rootTap = new RootTap();

    graph.addVertex( rootTap );

    Set<Tap> taps = graph.vertexSet();

    for( Tap tap : taps )
      {
      if( !( tap instanceof RootTap ) && graph.inDegreeOf( tap ) == 0 )
        graph.addEdge( rootTap, tap );
      }

    return rootTap;
    }

  private void makeGraph( SimpleDirectedGraph<Tap, Flow.FlowHolder> graph, Flow[] flows )
    {
    for( Flow flow : flows )
      {
      Collection sources = flow.getSources().values();
      Collection sinks = flow.getSinks().values();

      for( Object source : sources )
        graph.addVertex( (Tap) source );

      for( Object sink : sinks )
        graph.addVertex( (Tap) sink );

      for( Object source : sources )
        {
        for( Object sink : sinks )
          graph.addEdge( (Tap) source, (Tap) sink, flow.getHolder() );
        }
      }
    }

  /** Specialized type of {@link Tap} that is the root. */
  static class RootTap extends Tap
    {
    /** Field serialVersionUID */
    private static final long serialVersionUID = 1L;

    /** @see Tap#getPath() */
    public Path getPath()
      {
      return null;
      }

    /** @see Tap#containsFile(JobConf, String) */
    public boolean containsFile( JobConf conf, String currentFile )
      {
      return false;
      }

    /** @see Tap#deletePath(JobConf) */
    public boolean deletePath( JobConf conf ) throws IOException
      {
      return false;
      }

    /** @see Tap#pathExists(JobConf) */
    public boolean pathExists( JobConf conf ) throws IOException
      {
      return false;
      }

    /** @see Tap#getPathModified(JobConf) */
    public long getPathModified( JobConf conf ) throws IOException
      {
      return 0;
      }
    }

  }
