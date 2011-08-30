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

package cascading.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.util.Def;

/**
 *
 */
public class FlowDef extends Def<FlowDef>
  {
  protected Map<String, Tap> sources = new HashMap<String, Tap>();
  protected Map<String, Tap> sinks = new HashMap<String, Tap>();
  protected Map<String, Tap> traps = new HashMap<String, Tap>();

  protected List<Pipe> tails = new ArrayList<Pipe>();

  public static FlowDef flowDef()
    {
    return new FlowDef();
    }

  public FlowDef()
    {
    }

  public Map<String, Tap> getSources()
    {
    return sources;
    }

  public Map<String, Tap> getSourcesCopy()
    {
    return new HashMap<String, Tap>( sources );
    }

  public FlowDef addSource( String name, Tap source )
    {
    sources.put( name, source );
    return this;
    }

  public FlowDef addSource( Pipe head, Tap source )
    {
    sources.put( head.getName(), source );
    return this;
    }

  public FlowDef addSources( Map<String, Tap> sources )
    {
    if( sources != null )
      this.sources.putAll( sources );

    return this;
    }

  public Map<String, Tap> getSinks()
    {
    return sinks;
    }

  public Map<String, Tap> getSinksCopy()
    {
    return new HashMap<String, Tap>( sinks );
    }

  public FlowDef addSink( String name, Tap sink )
    {
    sinks.put( name, sink );
    return this;
    }

  public FlowDef addSink( Pipe head, Tap sink )
    {
    sinks.put( head.getName(), sink );
    return this;
    }

  public FlowDef addSinks( Map<String, Tap> sinks )
    {
    if( sinks != null )
      this.sinks.putAll( sinks );

    return this;
    }

  public Map<String, Tap> getTraps()
    {
    return traps;
    }

  public Map<String, Tap> getTrapsCopy()
    {
    return new HashMap<String, Tap>( traps );
    }

  public FlowDef addTrap( String name, Tap trap )
    {
    traps.put( name, trap );
    return this;
    }

  public FlowDef addTrap( Pipe head, Tap trap )
    {
    traps.put( head.getName(), trap );
    return this;
    }

  public FlowDef addTraps( Map<String, Tap> traps )
    {
    if( traps != null )
      this.traps.putAll( traps );

    return this;
    }

  public List<Pipe> getTails()
    {
    return tails;
    }

  public Pipe[] getTailsArray()
    {
    return tails.toArray( new Pipe[ tails.size() ] );
    }

  public FlowDef addTail( Pipe tail )
    {
    if( tail != null )
      this.tails.add( tail );

    return this;
    }

  public FlowDef addTails( Collection<Pipe> tails )
    {
    for( Pipe tail : tails )
      addTail( tail );

    return this;
    }

  public FlowDef addTails( Pipe... tails )
    {
    for( Pipe tail : tails )
      addTail( tail );

    return this;
    }
  }
