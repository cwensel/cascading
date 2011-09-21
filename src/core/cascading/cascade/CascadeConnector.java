/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.cascade;

import java.beans.ConstructorProperties;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.util.Util;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.cascade.CascadeDef.cascadeDef;

/**
 * Class CascadeConnector is used to construct a new {@link Cascade} instance from a collection of {@link Flow} instance.
 * <p/>
 * Note order is not significant when adding passing Flow instances to the {@code connect}
 * method. This connector will order them based on their dependencies, if any.
 */
public class CascadeConnector
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( CascadeConnector.class );

  /** Field properties */
  private Map<Object, Object> properties;

  /** Constructor CascadeConnector creates a new CascadeConnector instance. */
  public CascadeConnector()
    {
    }

  /**
   * Constructor CascadeConnector creates a new CascadeConnector instance.
   *
   * @param properties of type Map<Object, Object>
   */
  @ConstructorProperties({"properties"})
  public CascadeConnector( Map<Object, Object> properties )
    {
    this.properties = properties;
    }

  /**
   * Given any number of {@link Flow} objects, it will connect them and return a new {@link Cascade} instance. The name
   * of the Cascade is derived from the given Flow instances.
   *
   * @param flows of type Collection<Flow>
   * @return Cascade
   */
  public Cascade connect( Collection<Flow> flows )
    {
    return connect( null, flows.toArray( new Flow[ flows.size() ] ) );
    }

  /**
   * Given any number of {@link Flow} objects, it will connect them and return a new {@link Cascade} instance.
   *
   * @param name  of type String
   * @param flows of type Collection<Flow>
   * @return Cascade
   */
  public Cascade connect( String name, Collection<Flow> flows )
    {
    return connect( name, flows.toArray( new Flow[ flows.size() ] ) );
    }

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

    CascadeDef cascadeDef = cascadeDef()
      .setName( name )
      .addFlows( flows );

    return connect( cascadeDef );
    }

  public Cascade connect( CascadeDef cascadeDef )
    {
    SimpleDirectedGraph<String, Flow.FlowHolder> tapGraph = new SimpleDirectedGraph<String, Flow.FlowHolder>( Flow.FlowHolder.class );
    SimpleDirectedGraph<Flow, Integer> flowGraph = new SimpleDirectedGraph<Flow, Integer>( Integer.class );

    makeTapGraph( tapGraph, cascadeDef.getFlowsArray() );
    makeFlowGraph( flowGraph, tapGraph );

    verifyNoCycles( flowGraph );

    return new Cascade( cascadeDef, properties, flowGraph, tapGraph );
    }

  private String makeName( Flow[] flows )
    {
    String[] names = new String[ flows.length ];

    for( int i = 0; i < flows.length; i++ )
      names[ i ] = flows[ i ].getName();

    return Util.join( names, "+" );
    }

  private void verifyNoCycles( SimpleDirectedGraph<Flow, Integer> flowGraph )
    {
    Set<Flow> flows = new HashSet<Flow>();

    TopologicalOrderIterator<Flow, Integer> topoIterator = new TopologicalOrderIterator<Flow, Integer>( flowGraph );

    while( topoIterator.hasNext() )
      flows.add( topoIterator.next() );

    if( flows.size() != flowGraph.vertexSet().size() )
      throw new CascadeException( "there are likely cycles in the set of given flows, topological iterator cannot traverse flows with cycles" );
    }

  private void makeTapGraph( SimpleDirectedGraph<String, Flow.FlowHolder> tapGraph, Flow[] flows )
    {
    for( Flow flow : flows )
      {
      LinkedList<Tap> sources = new LinkedList<Tap>( flow.getSourcesCollection() );
      LinkedList<Tap> sinks = new LinkedList<Tap>( flow.getSinksCollection() );

      unwrapCompositeTaps( sources );
      unwrapCompositeTaps( sinks );

      for( Tap source : sources )
        tapGraph.addVertex( getFullPath( flow, source ) );

      for( Tap sink : sinks )
        tapGraph.addVertex( getFullPath( flow, sink ) );

      for( Tap source : sources )
        {
        for( Tap sink : sinks )
          addEdgeFor( tapGraph, flow, source, sink );
        }
      }
    }

  private void addEdgeFor( SimpleDirectedGraph<String, Flow.FlowHolder> tapGraph, Flow flow, Tap source, Tap sink )
    {
    try
      {
      tapGraph.addEdge( getFullPath( flow, source ), getFullPath( flow, sink ), flow.getHolder() );
      }
    catch( IllegalArgumentException exception )
      {
      throw new CascadeException( "no loops allowed in cascade, flow: " + flow.getName() + ", source: " + source + ", sink: " + sink );
      }
    }

  private String getFullPath( Flow flow, Tap tap )
    {
    return tap.getFullIdentifier( flow.getConfig() );
    }

  private void unwrapCompositeTaps( LinkedList<Tap> taps )
    {
    ListIterator<Tap> iterator = taps.listIterator();

    while( iterator.hasNext() )
      {
      Tap tap = iterator.next();

      if( tap instanceof CompositeTap )
        {
        iterator.remove();

        for( Tap childTap : ( (CompositeTap) tap ).getChildTaps() )
          iterator.add( childTap );
        }
      }
    }

  private void makeFlowGraph( SimpleDirectedGraph<Flow, Integer> jobGraph, SimpleDirectedGraph<String, Flow.FlowHolder> tapGraph )
    {
    Set<String> identifiers = tapGraph.vertexSet();

    int count = 0;

    for( String source : identifiers )
      {
      if( LOG.isDebugEnabled() )
        LOG.debug( "handling flow source: {}", source );

      List<String> sinks = Graphs.successorListOf( tapGraph, source );

      for( String sink : sinks )
        {
        if( LOG.isDebugEnabled() )
          LOG.debug( "handling flow path: {} -> {}", source, sink );

        Flow flow = tapGraph.getEdge( source, sink ).flow;

        jobGraph.addVertex( flow );

        Set<Flow.FlowHolder> previous = tapGraph.incomingEdgesOf( source );

        for( Flow.FlowHolder previousFlow : previous )
          {
          jobGraph.addVertex( previousFlow.flow );

          if( jobGraph.getEdge( previousFlow.flow, flow ) != null )
            continue;

          if( !jobGraph.addEdge( previousFlow.flow, flow, count++ ) )
            throw new CascadeException( "unable to add path between: " + previousFlow.flow.getName() + " and: " + flow.getName() );
          }
        }
      }
    }
  }
