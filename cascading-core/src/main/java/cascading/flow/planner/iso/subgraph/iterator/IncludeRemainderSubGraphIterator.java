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

package cascading.flow.planner.iso.subgraph.iterator;

import java.util.HashSet;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementMaskSubGraph;
import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.subgraph.SubGraphIterator;
import cascading.util.EnumMultiMap;

/**
 *
 */
public class IncludeRemainderSubGraphIterator implements SubGraphIterator
  {
  SubGraphIterator parentIterator;

  Set<FlowElement> maskedElements = new HashSet<>();
  Set<Scope> maskedScopes = new HashSet<>();

  {
  // creates consistent results across SubGraphIterators
  maskedElements.add( Extent.head );
  maskedElements.add( Extent.tail );
  }

  public IncludeRemainderSubGraphIterator( SubGraphIterator parentIterator )
    {
    this.parentIterator = parentIterator;
    }

  @Override
  public ElementGraph getElementGraph()
    {
    return parentIterator.getElementGraph();
    }

  @Override
  public EnumMultiMap getAnnotationMap( ElementAnnotation[] annotations )
    {
    return parentIterator.getAnnotationMap( annotations );
    }

  @Override
  public boolean hasNext()
    {
    return parentIterator.hasNext();
    }

  @Override
  public ElementGraph next()
    {
    ElementGraph next = parentIterator.next();

    if( parentIterator.hasNext() )
      {
      maskedElements.addAll( next.vertexSet() );
      maskedScopes.addAll( next.edgeSet() ); // catches case with no elements on path

      return next;
      }

    maskedElements.removeAll( next.vertexSet() );
    maskedScopes.removeAll( next.edgeSet() );

    return new ElementMaskSubGraph( parentIterator.getElementGraph(), maskedElements, maskedScopes );
    }

  @Override
  public void remove()
    {
    parentIterator.remove();
    }
  }
