/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner.graph;

import cascading.flow.FlowElement;
import cascading.flow.planner.ElementGraphs;
import cascading.flow.planner.Scope;
import cascading.util.EnumMultiMap;
import cascading.util.Util;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;

/**
 *
 */
public class ElementDirectedGraph extends SimpleDirectedGraph<FlowElement, Scope> implements ElementGraph, AnnotatedGraph
  {
  protected EnumMultiMap annotations;

  public ElementDirectedGraph()
    {
    super( Scope.class );
    }

  public ElementDirectedGraph( ElementGraph parent )
    {
    this();

    if( parent == null )
      return;

    Graphs.addGraph( this, parent );

    if( parent instanceof AnnotatedGraph && ( ( (AnnotatedGraph) parent ) ).hasAnnotations() )
      this.getAnnotations().addAll( ( (AnnotatedGraph) parent ).getAnnotations() );
    }

  public ElementDirectedGraph( ElementGraph parent, EnumMultiMap annotations )
    {
    this( parent );

    this.getAnnotations().addAll( annotations );
    }

  @Override
  public ElementGraph copyGraph()
    {
    return new ElementDirectedGraph( this );
    }

  @Override
  public void writeDOT( String filename )
    {
    boolean success = ElementGraphs.printElementGraph( filename, this, null );

    if( success )
      Util.writePDF( filename );
    }

  @Override
  public boolean hasAnnotations()
    {
    return annotations != null && !annotations.isEmpty();
    }

  @Override
  public EnumMultiMap getAnnotations()
    {
    if( annotations == null )
      annotations = new EnumMultiMap();

    return annotations;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;

    if( !super.equals( object ) )
      return false;

    AnnotatedGraph that = (AnnotatedGraph) object;

    if( annotations != null ? !annotations.equals( that.getAnnotations() ) : that.getAnnotations() != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( annotations != null ? annotations.hashCode() : 0 );
    return result;
    }
  }
