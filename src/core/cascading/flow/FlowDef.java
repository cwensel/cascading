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
