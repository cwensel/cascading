/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.cascade;

import java.beans.ConstructorProperties;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import cascading.cascade.planner.FlowGraph;
import cascading.cascade.planner.TapGraph;
import cascading.flow.BaseFlow;
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
 * Class CascadeConnector is used to construct a new {@link Cascade} instance from a collection of {@link cascading.flow.Flow} instance.
 * <p/>
 * Note order is not significant when adding passing Flow instances to the {@code connect}
 * method. This connector will order them based on their dependencies, if any.
 * <p/>
 * One Flow is dependent on another if the first sinks (produces) output that the second Flow sources (consumes) as
 * input. A sink and source are considered equivalent if the fully qualified identifier, typically {@link Tap#getFullIdentifier(Object)}
 * from either are {@code equals()}.
 * <p/>
 * <p/>
 * Note that checkpoint sink Taps from an upstream Flow may be the sources to downstream Flow instances.
 * <p/>
 * The {@link CascadeDef} is a convenience class for dynamically defining a Cascade that can be passed to the
 * {@link CascadeConnector#connect(CascadeDef)} method.
 * <p/>
 * Use the {@link CascadeProps} fluent helper class to create global properties to pass to the CascadeConnector
 * constructor.
 *
 * @see CascadeDef
 * @see CascadeProps
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
   * Given any number of {@link cascading.flow.Flow} objects, it will connect them and return a new {@link Cascade} instance. The name
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
   * Given any number of {@link cascading.flow.Flow} objects, it will connect them and return a new {@link Cascade} instance.
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
   * Given any number of {@link cascading.flow.Flow} objects, it will connect them and return a new {@link Cascade} instance. The name
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
   * Given any number of {@link cascading.flow.Flow} objects, it will connect them and return a new {@link Cascade} instance.
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
    TapGraph tapGraph = new TapGraph();
    FlowGraph flowGraph = new FlowGraph();

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

  private void makeTapGraph( SimpleDirectedGraph<String, BaseFlow.FlowHolder> tapGraph, Flow[] flows )
    {
    for( Flow flow : flows )
      {
      LinkedList<Tap> sources = new LinkedList<Tap>( flow.getSourcesCollection() );
      LinkedList<Tap> sinks = new LinkedList<Tap>( flow.getSinksCollection() );

      sinks.addAll( flow.getCheckpointsCollection() );

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

  private void addEdgeFor( SimpleDirectedGraph<String, BaseFlow.FlowHolder> tapGraph, Flow flow, Tap source, Tap sink )
    {
    try
      {
      tapGraph.addEdge( getFullPath( flow, source ), getFullPath( flow, sink ), ( (BaseFlow) flow ).getHolder() );
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

        Iterator<Tap> childTaps = ( (CompositeTap) tap ).getChildTaps();

        while( childTaps.hasNext() )
          {
          iterator.add( childTaps.next() );
          iterator.previous(); // force cursor backward
          }
        }
      }
    }

  private void makeFlowGraph( SimpleDirectedGraph<Flow, Integer> jobGraph, SimpleDirectedGraph<String, BaseFlow.FlowHolder> tapGraph )
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

        Set<BaseFlow.FlowHolder> previous = tapGraph.incomingEdgesOf( source );

        for( BaseFlow.FlowHolder previousFlow : previous )
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
