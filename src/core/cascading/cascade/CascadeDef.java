/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
import cascading.util.Def;

/**
 *
 */
public class CascadeDef extends Def<CascadeDef>
  {
  Map<String, Flow> flows = new HashMap<String, Flow>();

  public static CascadeDef cascadeDef()
    {
    return new CascadeDef();
    }

  public Collection<Flow> getFlows()
    {
    return flows.values();
    }

  public Flow[] getFlowsArray()
    {
    return getFlows().toArray( new Flow[ flows.size() ] );
    }

  public CascadeDef addFlow( Flow flow )
    {
    if( flow == null )
      return this;

    if( flows.containsKey( flow.getName() ) )
      throw new CascadeException( "all flow names must be unique, found duplicate: " + flow.getName() );

    flows.put( flow.getName(), flow );

    return this;
    }

  public CascadeDef addFlows( Flow... flows )
    {
    for( Flow flow : flows )
      addFlow( flow );

    return this;
    }

  public CascadeDef addFlows( Collection<Flow> flows )
    {
    for( Flow flow : flows )
      addFlow( flow );

    return this;
    }
  }
