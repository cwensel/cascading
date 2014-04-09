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

package cascading.flow.planner;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.pipe.Group;
import cascading.tap.Tap;
import cascading.util.Util;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class ProcessGraph<Process extends ProcessModel> extends SimpleDirectedGraph<Process, Integer>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( ProcessGraph.class );

  protected Set<FlowElement> sourceElements = new HashSet<>();
  protected Set<FlowElement> sinkElements = new HashSet<>();
  private Set<Tap> sourceTaps;
  private Set<Tap> sinkTaps;
  protected Map<String, Tap> trapsMap = new HashMap<>();

  public ProcessGraph()
    {
    super( Integer.class );
    }

  @Override
  public boolean addVertex( Process process )
    {
    sourceElements.addAll( process.getSourceElements() );
    sinkElements.addAll( process.getSinkElements() );
    trapsMap.putAll( process.getTrapMap() );

    return super.addVertex( process );
    }

  protected void bindEdges()
    {
    int count = 0;
    for( Process sinkProcess : vertexSet() )
      {
      for( Process sourceProcess : vertexSet() )
        {
        if( sourceProcess == sinkProcess )
          continue;

        sourceElements.removeAll( sinkProcess.getSinkElements() );
        sinkElements.removeAll( sourceProcess.getSourceElements() );

        for( Object sink : sinkProcess.getSinks() )
          {
          if( sourceProcess.getSources().contains( sink ) )
            addEdge( sinkProcess, sourceProcess, count++ );
          }
        }
      }
    }

  public Set<FlowElement> getSourceElements()
    {
    return sourceElements;
    }

  public Set<FlowElement> getSinkElements()
    {
    return sinkElements;
    }

  public Set<Tap> getSourceTaps()
    {
    if( sourceTaps != null )
      return sourceTaps;

    sourceTaps = new HashSet<>();

    for( FlowElement sourceElement : getSourceElements() )
      {
      if( sourceElement instanceof Tap )
        sourceTaps.add( (Tap) sourceElement );
      }

    return sourceTaps;
    }

  public Set<Tap> getSinkTaps()
    {
    if( sinkTaps != null )
      return sinkTaps;

    sinkTaps = new HashSet<>();

    for( FlowElement sinkElement : getSinkElements() )
      {
      if( sinkElement instanceof Tap )
        sinkTaps.add( (Tap) sinkElement );
      }

    return sinkTaps;
    }

  public Map<String, Tap> getTrapsMap()
    {
    return trapsMap;
    }

  public Iterator<Process> getTopologicalIterator()
    {
    return new TopologicalOrderIterator<>( this, new PriorityQueue<>( 10, new Comparator<Process>()
    {
    @Override
    public int compare( Process lhs, Process rhs )
      {
      return Integer.valueOf( lhs.getSubmitPriority() ).compareTo( rhs.getSubmitPriority() );
      }
    } ) );
    }

  /**
   * Method writeDOT writes this element graph to a DOT file for easy visualization and debugging.
   *
   * @param filename of type String
   */
  public void writeDOT( String filename )
    {
    printProcessGraph( filename );
    }

  protected void printProcessGraph( String filename )
    {
    try
      {
      Writer writer = new FileWriter( filename );

      Util.writeDOT( writer, this, new IntegerNameProvider<Process>(), new VertexNameProvider<Process>()
      {
      public String getVertexName( Process process )
        {
        String name = "[" + process.getName() + "]";

        String sourceName = "";
        Set<Tap> sources = process.getSources();
        for( Tap source : sources )
          sourceName += "\\nsrc:[" + source.getIdentifier() + "]";

        if( sourceName.length() != 0 )
          name += sourceName;

        Collection<Group> groups = process.getGroups();

        for( Group group : groups )
          {
          String groupName = group.getName();

          if( groupName.length() != 0 )
            name += "\\ngrp:" + groupName;
          }

        Set<Tap> sinks = process.getSinks();
        String sinkName = "";
        for( Tap sink : sinks )
          sinkName = "\\nsnk:[" + sink.getIdentifier() + "]";

        if( sinkName.length() != 0 )
          name += sinkName;

        return name.replaceAll( "\"", "\'" );
        }
      }, null );

      writer.close();
      }
    catch( IOException exception )
      {
      LOG.error( "failed printing graph to: {}, with exception: {}", filename, exception );
      }
    }
  }
