/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.property.UnitOfWorkDef;
import cascading.tap.Tap;

/**
 * Class CascadeDef is a fluent interface for defining a {@link Cascade}.
 * <p>
 * This allows for ad-hoc building of Cascade data and meta-data like tags.
 * <p>
 * Instead of calling one of the {@link CascadeConnector} connect methods, {@link CascadeConnector#connect(CascadeDef)}
 * can be called.
 *
 * @see cascading.property.UnitOfWorkDef
 * @see cascading.flow.FlowDef
 */
public class CascadeDef extends UnitOfWorkDef<CascadeDef>
  {
  Map<String, Flow> flows = new HashMap<String, Flow>();
  int maxConcurrentFlows = -1;

  /**
   * Creates a new instance of a CascadeDef.
   *
   * @return a CascadeDef
   */
  public static CascadeDef cascadeDef()
    {
    return new CascadeDef();
    }

  /** Constructor CascadeDef creates a new CascadeDef instance. */
  public CascadeDef()
    {
    }

  /**
   * Method getFlows returns the flows of this CascadeDef object.
   *
   * @return the flows (type Collection) of this CascadeDef object.
   */
  public Collection<Flow> getFlows()
    {
    return flows.values();
    }

  /**
   * Method getFlowsArray returns the flows as an array of this CascadeDef object.
   *
   * @return the flowsArray (type Flow[]) of this CascadeDef object.
   */
  public Flow[] getFlowsArray()
    {
    return getFlows().toArray( new Flow[ flows.size() ] );
    }

  /**
   * Method addFlow adds a new {@link cascading.flow.Flow} instance that is intended to participate in a {@link Cascade}.
   *
   * @param flow of Flow
   * @return CascadeDef
   */
  public CascadeDef addFlow( Flow flow )
    {
    if( flow == null )
      return this;

    if( flows.containsKey( flow.getName() ) )
      throw new CascadeException( "all flow names must be unique, found duplicate: " + flow.getName() );

    Collection<Tap> sinks = flow.getSinksCollection();

    for( Tap sink : sinks )
      {
      String fullIdentifier = sink.getFullIdentifier( flow.getConfig() );

      for( Flow existingFlow : flows.values() )
        {
        Collection<Tap> existingSinks = existingFlow.getSinksCollection();

        for( Tap existingSink : existingSinks )
          {
          if( fullIdentifier.equals( existingSink.getFullIdentifier( existingFlow.getConfig() ) ) )
            throw new CascadeException( "the flow: " + flow.getName() + ", has a sink identifier: " + fullIdentifier + ", in common with the flow: " + existingFlow.getName() );
          }
        }
      }

    flows.put( flow.getName(), flow );

    return this;
    }

  /**
   * Method addFlows adds many new {@link cascading.flow.Flow} instances intended to participate in a {@link Cascade}.
   *
   * @param flows of Flow[]
   * @return CascadeDef
   */
  public CascadeDef addFlows( Flow... flows )
    {
    for( Flow flow : flows )
      addFlow( flow );

    return this;
    }

  /**
   * Method addFlows adds many new {@link cascading.flow.Flow} instances intended to participate in a {@link Cascade}.
   *
   * @param flows of Collection
   * @return CascadeDef
   */
  public CascadeDef addFlows( Collection<Flow> flows )
    {
    for( Flow flow : flows )
      addFlow( flow );

    return this;
    }

  public CascadeDef setMaxConcurrentFlows( int maxConcurrentFlows )
    {
    this.maxConcurrentFlows = maxConcurrentFlows;

    return this;
    }

  public int getMaxConcurrentFlows()
    {
    return maxConcurrentFlows;
    }
  }
