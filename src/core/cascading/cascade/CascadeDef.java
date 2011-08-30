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
