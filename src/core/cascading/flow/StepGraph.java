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

package cascading.flow;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tap.TempHfs;
import cascading.util.Util;
import org.apache.log4j.Logger;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

/** Class StepGraph is an internal representation of {@link FlowStep} instances. */
public class StepGraph extends SimpleDirectedGraph<FlowStep, Integer>
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( StepGraph.class );

  /** Constructor StepGraph creates a new StepGraph instance. */
  StepGraph()
    {
    super( Integer.class );
    }

  /**
   * Constructor StepGraph creates a new StepGraph instance.
   *
   * @param elementGraph of type ElementGraph
   * @param traps        of type Map<String, Tap>
   */
  StepGraph( String flowName, ElementGraph elementGraph, Map<String, Tap> traps )
    {
    this();

    makeStepGraph( flowName, elementGraph, traps );

    validateGraph( traps );
    }

  private void validateGraph( Map<String, Tap> traps )
    {
    verifyTrapsAreUnique( traps );

    traps = new HashMap<String, Tap>( traps ); // make copy

    TopologicalOrderIterator<FlowStep, Integer> iterator = getTopologicalIterator();

    while( iterator.hasNext() )
      {
      FlowStep step = iterator.next();

      verifyTraps( traps, step.mapperTraps );
      verifyTraps( traps, step.reducerTraps );
      }
    }

  private void verifyTrapsAreUnique( Map<String, Tap> traps )
    {
    for( Tap tap : traps.values() )
      {
      if( Collections.frequency( traps.values(), tap ) != 1 )
        throw new PlannerException( "traps must be unique, cannot be reused on different branches: " + tap );
      }
    }

  private void verifyTraps( Map<String, Tap> traps, Map<String, Tap> map )
    {
    for( String name : map.keySet() )
      {
      if( !traps.containsKey( name ) )
        throw new PlannerException( "traps may not cross Map and Reduce boundaries: " + name );
      else
        traps.remove( name );
      }
    }

  /**
   * Method getCreateFlowStep ...
   *
   * @param flowName of type String
   * @param steps    of type Map<String, FlowStep>
   * @param sinkName of type String
   * @param numJobs  of type int
   * @return FlowStep
   */
  private FlowStep getCreateFlowStep( String flowName, Map<String, FlowStep> steps, String sinkName, int numJobs )
    {
    if( steps.containsKey( sinkName ) )
      return steps.get( sinkName );

    if( LOG.isDebugEnabled() )
      LOG.debug( "creating step: " + sinkName );

    FlowStep step = new FlowStep( makeStepName( steps, numJobs, sinkName ) );

    step.setParentFlowName( flowName );

    steps.put( sinkName, step );

    return step;
    }

  private String makeStepName( Map<String, FlowStep> steps, int numJobs, String sinkPath )
    {
    // todo make the long form optional via a property
    if( sinkPath.length() > 150 )
      sinkPath = sinkPath.substring( sinkPath.length() - 150 );

    return String.format( "(%d/%d) %s", steps.size() + 1, numJobs, sinkPath );
    }

  /**
   * Creates the map reduce step graph.
   *
   * @param flowName
   * @param elementGraph
   * @param traps
   */
  private void makeStepGraph( String flowName, ElementGraph elementGraph, Map<String, Tap> traps )
    {
    SimpleDirectedGraph<Tap, Integer> tapGraph = elementGraph.makeTapGraph();

    int numJobs = countNumJobs( tapGraph );

    Map<String, FlowStep> steps = new LinkedHashMap<String, FlowStep>();
    TopologicalOrderIterator<Tap, Integer> topoIterator = new TopologicalOrderIterator<Tap, Integer>( tapGraph );
    int count = 0;

    while( topoIterator.hasNext() )
      {
      Tap source = topoIterator.next();

      if( LOG.isDebugEnabled() )
        LOG.debug( "handling source: " + source );

      List<Tap> sinks = Graphs.successorListOf( tapGraph, source );

      for( Tap sink : sinks )
        {
        if( LOG.isDebugEnabled() )
          LOG.debug( "handling path: " + source + " -> " + sink );

        FlowStep step = getCreateFlowStep( flowName, steps, sink.toString(), numJobs );

        addVertex( step );

        if( steps.containsKey( source.toString() ) )
          addEdge( steps.get( source.toString() ), step, count++ );

        // support multiple paths from source to sink
        // this allows for self joins on groups, even with different operation stacks between them
        // note we must ignore paths with intermediate taps
        List<GraphPath<FlowElement, Scope>> paths = elementGraph.getAllShortestPathsBetween( source, sink );

        for( GraphPath<FlowElement, Scope> path : paths )
          {
          if( pathContainsTap( path ) )
            continue;

          List<Scope> scopes = path.getEdgeList();
          String sourceName = scopes.get( 0 ).getName(); // root node of the shortest path

          step.sources.put( (Tap) source, sourceName );
          step.sink = sink;

          if( step.sink.isWriteDirect() )
            step.tempSink = new TempHfs( sink.getPath().toUri().getPath() );

          FlowElement lhs = source;

          step.graph.addVertex( lhs );

          boolean onMapSide = true;

          for( Scope scope : scopes )
            {
            FlowElement rhs = elementGraph.getEdgeTarget( scope );

            step.graph.addVertex( rhs );
            step.graph.addEdge( lhs, rhs, scope );

            if( rhs instanceof Group )
              {
              step.group = (Group) rhs;
              onMapSide = false;
              }
            else if( rhs instanceof Pipe ) // add relevant traps to step
              {
              String name = ( (Pipe) rhs ).getName();

              if( traps.containsKey( name ) )
                {
                if( onMapSide )
                  step.mapperTraps.put( name, traps.get( name ) );
                else
                  step.reducerTraps.put( name, traps.get( name ) );
                }
              }

            lhs = rhs;
            }
          }
        }
      }
    }

  private int countNumJobs( SimpleDirectedGraph<Tap, Integer> tapGraph )
    {
    Set<Tap> vertices = tapGraph.vertexSet();
    int count = 0;

    for( Tap vertice : vertices )
      {
      if( tapGraph.inDegreeOf( vertice ) != 0 )
        count++;
      }

    return count;
    }

  private boolean pathContainsTap( GraphPath<FlowElement, Scope> path )
    {
    List<FlowElement> flowElements = Graphs.getPathVertexList( path );

    // first and last are taps, if we find more than 2, return false
    int count = 0;

    for( FlowElement flowElement : flowElements )
      {
      if( flowElement instanceof Tap )
        count++;
      }

    return count > 2;
    }

  public TopologicalOrderIterator<FlowStep, Integer> getTopologicalIterator()
    {
    return new TopologicalOrderIterator<FlowStep, Integer>( this );
    }

  /**
   * Method writeDOT writes this element graph to a DOT file for easy vizualization and debugging.
   *
   * @param filename of type String
   */
  public void writeDOT( String filename )
    {
    printElementGraph( filename );
    }

  protected void printElementGraph( String filename )
    {
    try
      {
      Writer writer = new FileWriter( filename );

      Util.writeDOT( writer, this, new IntegerNameProvider<FlowStep>(), new VertexNameProvider<FlowStep>()
      {
      public String getVertexName( FlowStep object )
        {
        String sourceName = "";

        for( Tap source : object.sources.keySet() )
          {
          if( source instanceof TempHfs )
            continue;

          sourceName += "[" + source.getPath() + "]";
          }

        String sinkName = object.sink instanceof TempHfs ? "" : "[" + object.sink.getPath() + "]";

        String groupName = object.group == null ? "" : object.group.getName();

        String name = "[" + object.getName() + "]";

        if( sourceName.length() != 0 )
          name += "\\nsrc:" + sourceName;

        if( groupName.length() != 0 )
          name += "\\n" + groupName;

        if( sinkName.length() != 0 )
          name += "\\nsnk:" + sinkName;

        return name.replaceAll( "\"", "\'" );
        }
      }, null );

      writer.close();
      }
    catch( IOException exception )
      {
      exception.printStackTrace();
      }
    }

  }
