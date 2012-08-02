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

package cascading.cascade.planner;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

import cascading.cascade.CascadeException;
import cascading.flow.BaseFlow;
import cascading.flow.Flow;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import org.jgrapht.graph.SimpleDirectedGraph;

/**
 *
 */
public abstract class TopologyGraph<Vertex> extends SimpleDirectedGraph<Vertex, BaseFlow.FlowHolder>
  {
  public TopologyGraph( Flow... flows )
    {
    super( BaseFlow.FlowHolder.class );

    makeGraph( flows );
    }

  private void makeGraph( Flow[] flows )
    {
    for( Flow flow : flows )
      {
      LinkedList<Tap> sources = new LinkedList<Tap>( flow.getSourcesCollection() );
      LinkedList<Tap> sinks = new LinkedList<Tap>( flow.getSinksCollection() );

      sinks.addAll( flow.getCheckpointsCollection() );

      unwrapCompositeTaps( sources );
      unwrapCompositeTaps( sinks );

      for( Tap source : sources )
        addVertex( getVertex( flow, source ) );

      for( Tap sink : sinks )
        addVertex( getVertex( flow, sink ) );

      for( Tap source : sources )
        {
        for( Tap sink : sinks )
          addEdgeFor( flow, source, sink );
        }
      }
    }

  private void addEdgeFor( Flow flow, Tap source, Tap sink )
    {
    try
      {
      addEdge( getVertex( flow, source ), getVertex( flow, sink ), ( (BaseFlow) flow ).getHolder() );
      }
    catch( IllegalArgumentException exception )
      {
      throw new CascadeException( "no loops allowed in cascade, flow: " + flow.getName() + ", source: " + source + ", sink: " + sink );
      }
    }

  abstract protected Vertex getVertex( Flow flow, Tap tap );

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
  }
