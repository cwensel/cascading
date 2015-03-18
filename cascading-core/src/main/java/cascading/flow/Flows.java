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

package cascading.flow;

import java.util.Map;
import java.util.Set;

import cascading.flow.planner.FlowStepJob;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.process.FlowStepGraph;
import cascading.tap.Tap;

/** Flows is a utility helper class. */
public final class Flows
  {
  private Flows()
    {
    }

  public static Map<String, FlowStepJob> getJobsMap( Flow flow )
    {
    return ( (BaseFlow) flow ).getJobsMap();
    }

  public static FlowElementGraph getPipeGraphFrom( Flow flow )
    {
    return ( (BaseFlow) flow ).getPipeGraph();
    }

  public static FlowStepGraph getStepGraphFrom( Flow flow )
    {
    return ( (BaseFlow) flow ).getFlowStepGraph();
    }

  public static String getNameOrID( Flow flow )
    {
    if( flow == null )
      return null;

    if( flow.getName() != null )
      return flow.getName();

    return flow.getID().substring( 0, 6 );
    }

  public static Tap getTapForID( Set<Tap> taps, String id )
    {
    for( Tap tap : taps )
      {
      if( Tap.id( tap ).equals( id ) )
        return tap;
      }

    return null;
    }

  public static FlowElement getFlowElementForID( Set<FlowElement> flowElements, String id )
    {
    for( FlowElement flowElement : flowElements )
      {
      if( FlowElements.id( flowElement ).equals( id ) )
        return flowElement;
      }

    return null;
    }

  public static FlowDef copy( FlowDef flowDef, Map<String, Tap> sources, Map<String, Tap> sinks, Map<String, Tap> traps, Map<String, Tap> checkpoints )
    {
    return new FlowDef( flowDef, sources, sinks, traps, checkpoints );
    }
  }
