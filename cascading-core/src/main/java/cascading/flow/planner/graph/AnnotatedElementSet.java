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

package cascading.flow.planner.graph;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;

/**
 *
 */
public class AnnotatedElementSet implements Serializable
  {
  private Map<Enum, Set<FlowElement>> annotations = new IdentityHashMap<>();

  public AnnotatedElementSet()
    {
    }

  public AnnotatedElementSet( AnnotatedElementSet annotations )
    {
    addAnnotations( annotations );
    }

  public Set<Enum> getAllAnnotations()
    {
    return annotations.keySet();
    }

  public void addAnnotations( AnnotatedElementSet annotations )
    {
    if( annotations == null )
      return;

    for( Map.Entry<Enum, Set<FlowElement>> entry : annotations.annotations.entrySet() )
      addAnnotations( entry.getKey(), entry.getValue() );
    }

  public void addAnnotations( Enum annotation, FlowElement... flowElements )
    {
    addAnnotations( annotation, Arrays.asList( flowElements ) );
    }

  public void addAnnotations( Enum annotation, Collection<FlowElement> flowElements )
    {
    if( !annotations.containsKey( annotation ) )
      annotations.put( annotation, new HashSet<FlowElement>() );

    annotations.get( annotation ).addAll( flowElements );
    }

  public Set<FlowElement> getFlowElements()
    {
    if( annotations.isEmpty() )
      return Collections.emptySet();

    Set<FlowElement> results = new HashSet<>();

    for( Set<FlowElement> flowElements : annotations.values() )
      results.addAll( flowElements );

    return results;
    }

  public Set<FlowElement> getFlowElementsFor( Enum annotation )
    {
    Set<FlowElement> flowElements = annotations.get( annotation );

    if( flowElements == null )
      return Collections.emptySet();

    return flowElements;
    }

  public Set<Enum> getAnnotationsFor( FlowElement flowElement )
    {
    Set<Enum> results = new HashSet<>();

    for( Map.Entry<Enum, Set<FlowElement>> entry : annotations.entrySet() )
      {
      if( entry.getValue().contains( flowElement ) )
        results.add( entry.getKey() );
      }

    return results;
    }

  public boolean hasAnnotation( FlowElement flowElement )
    {
    Set<Enum> annotations = getAnnotationsFor( flowElement );

    return annotations != null && !annotations.isEmpty();
    }

  public boolean hasAnnotation( Enum annotation, FlowElement flowElement )
    {
    Set<FlowElement> flowElements = annotations.get( annotation );

    return flowElements != null && flowElements.contains( flowElement );
    }

  public boolean isEmpty()
    {
    return annotations.isEmpty();
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "AnnotatedElementSet{" );
    sb.append( "annotations=" ).append( annotations );
    sb.append( '}' );
    return sb.toString();
    }
  }
