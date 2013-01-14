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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowElement;
import cascading.pipe.Group;
import cascading.pipe.Splice;
import cascading.tap.Tap;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.KShortestPaths;
import org.jgrapht.graph.SimpleDirectedGraph;

/**
 *
 */
public class ElementGraphs
  {
  /**
   * Method getAllShortestPathsBetween ...
   *
   * @param graph
   * @param from  of type FlowElement
   * @param to    of type FlowElement
   * @return List<GraphPath<FlowElement, Scope>>
   */
  public static List<GraphPath<FlowElement, Scope>> getAllShortestPathsBetween( SimpleDirectedGraph<FlowElement, Scope> graph, FlowElement from, FlowElement to )
    {
    List<GraphPath<FlowElement, Scope>> paths = new KShortestPaths<FlowElement, Scope>( graph, from, Integer.MAX_VALUE ).getPaths( to );

    if( paths == null )
      return new ArrayList<GraphPath<FlowElement, Scope>>();

    return paths;
    }

  public static List<List<FlowElement>> asPathList( List<GraphPath<FlowElement, Scope>> paths )
    {
    List<List<FlowElement>> results = new LinkedList<List<FlowElement>>();

    if( paths == null )
      return results;

    for( GraphPath<FlowElement, Scope> path : paths )
      results.add( Graphs.getPathVertexList( path ) );

    return results;
    }

  /**
   * All paths that lead from to to without crossing a Tap/Group boundary
   *
   * @param graph
   * @param from
   * @param to
   * @return of type List
   */
  public static List<GraphPath<FlowElement, Scope>> getAllDirectPathsBetween( SimpleDirectedGraph<FlowElement, Scope> graph, FlowElement from, FlowElement to )
    {
    List<GraphPath<FlowElement, Scope>> paths = getAllShortestPathsBetween( graph, from, to );
    List<GraphPath<FlowElement, Scope>> results = new ArrayList<GraphPath<FlowElement, Scope>>( paths );

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      List<FlowElement> pathVertexList = Graphs.getPathVertexList( path );

      for( int i = 1; i < pathVertexList.size(); i++ ) // skip the from, its a Tap or Group
        {
        FlowElement flowElement = pathVertexList.get( i );

        if( flowElement instanceof Tap || flowElement instanceof Group )
          {
          results.remove( path );
          break;
          }
        }
      }

    return results;
    }

  public static int countTypesBetween( SimpleDirectedGraph<FlowElement, Scope> graph, FlowElement from, Splice to, Class type )
    {
    List<GraphPath<FlowElement, Scope>> paths = getAllDirectPathsBetween( graph, from, to );

    int count = 0;

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      if( hasIntermediateTap( path, from ) )
        continue;

      List<FlowElement> flowElements = Graphs.getPathVertexList( path );

      for( FlowElement flowElement : flowElements )
        {
        if( type.isInstance( flowElement ) && flowElement != to )
          count++;
        }
      }

    return count;
    }

  /**
   * for every incoming stream to the splice, gets the count of paths.
   * <p/>
   * covers the case where a source may cross multiple joins to the current join and still land
   * on the lhs or rhs.
   *
   * @param graph
   * @param from
   * @param to
   * @return of type Map
   */
  public static Map<Integer, Integer> countOrderedDirectPathsBetween( SimpleDirectedGraph<FlowElement, Scope> graph, FlowElement from, Splice to )
    {
    return countOrderedDirectPathsBetween( graph, from, to, false );
    }

  public static Map<Integer, Integer> countOrderedDirectPathsBetween( SimpleDirectedGraph<FlowElement, Scope> graph, FlowElement from, Splice to, boolean skipTaps )
    {
    List<GraphPath<FlowElement, Scope>> paths = getAllDirectPathsBetween( graph, from, to );

    Map<Integer, Integer> results = new HashMap<Integer, Integer>();

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      if( skipTaps && hasIntermediateTap( path, from ) )
        continue;

      pathPositionInto( results, path, to );
      }

    return results;
    }

  public static boolean isBothAccumulatedAndStreamedPath( Map<Integer, Integer> pathCounts )
    {
    return pathCounts.size() > 1 && pathCounts.containsKey( 0 );
    }

  public static boolean isOnlyStreamedPath( Map<Integer, Integer> pathCounts )
    {
    return pathCounts.size() == 1 && pathCounts.containsKey( 0 );
    }

  public static boolean isOnlyAccumulatedPath( Map<Integer, Integer> pathCounts )
    {
    return pathCounts.size() >= 1 && !pathCounts.containsKey( 0 );
    }

  public static int countPaths( Map<Integer, Integer> pathCounts )
    {
    int count = 0;

    for( Integer integer : pathCounts.values() )
      count += integer;

    return count;
    }

  private static boolean hasIntermediateTap( GraphPath<FlowElement, Scope> path, FlowElement from )
    {
    List<FlowElement> flowElements = Graphs.getPathVertexList( path );

    for( FlowElement flowElement : flowElements )
      {
      if( flowElement instanceof Tap && flowElement != from )
        return true;
      }

    return false;
    }

  public static int pathPositionInto( GraphPath<FlowElement, Scope> path, Splice to )
    {
    List<FlowElement> flowElements = Graphs.getPathVertexList( path );
    List<Scope> scopes = path.getEdgeList();
    int index = flowElements.indexOf( to );

    return to.getPipePos().get( scopes.get( index - 1 ).getName() );
    }

  private static Map<Integer, Integer> pathPositionInto( Map<Integer, Integer> results, GraphPath<FlowElement, Scope> path, Splice to )
    {
    List<Scope> scopes = path.getEdgeList();

    Scope lastScope = scopes.get( scopes.size() - 1 );

    Integer pos = to.getPipePos().get( lastScope.getName() );

    if( results.containsKey( pos ) )
      results.put( pos, results.get( pos ) + 1 );
    else
      results.put( pos, 1 );

    return results;
    }
  }
