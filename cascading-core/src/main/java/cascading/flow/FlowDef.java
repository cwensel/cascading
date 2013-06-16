/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.operation.AssertionLevel;
import cascading.operation.DebugLevel;
import cascading.pipe.Checkpoint;
import cascading.pipe.Pipe;
import cascading.property.UnitOfWorkDef;
import cascading.tap.Tap;

/**
 * Class FlowDef is a fluent interface for defining a {@link Flow}.
 * <p/>
 * This allows for ad-hoc building of Flow data and meta-data like tags.
 * <p/>
 * Instead of calling one of the {@link FlowConnector} connect methods, {@link FlowConnector#connect(FlowDef)}
 * can be called.
 */
public class FlowDef extends UnitOfWorkDef<FlowDef>
  {
  protected Map<String, Tap> sources = new HashMap<String, Tap>();
  protected Map<String, Tap> sinks = new HashMap<String, Tap>();
  protected Map<String, Tap> traps = new HashMap<String, Tap>();
  protected Map<String, Tap> checkpoints = new HashMap<String, Tap>();

  protected List<String> classPath = new ArrayList<String>();
  protected List<Pipe> tails = new ArrayList<Pipe>();
  protected List<AssemblyPlanner> assemblyPlanners = new ArrayList<AssemblyPlanner>();

  protected AssertionLevel assertionLevel;
  protected DebugLevel debugLevel;

  protected String runID;

  /**
   * Creates a new instance of a FlowDef.
   *
   * @return a FlowDef
   */
  public static FlowDef flowDef()
    {
    return new FlowDef();
    }

  /** Constructor FlowDef creates a new FlowDef instance. */
  public FlowDef()
    {
    }

  /**
   * Method getAssemblyPlanners returns the current registered AssemblyPlanners.
   *
   * @return a List of AssemblyPlanner instances
   */
  public List<AssemblyPlanner> getAssemblyPlanners()
    {
    return assemblyPlanners;
    }

  /**
   * Method addAssemblyPlanner adds new AssemblyPlanner instances to be evaluated.
   *
   * @param assemblyPlanner of type AssemblyPlanner
   * @return a FlowDef
   */
  public FlowDef addAssemblyPlanner( AssemblyPlanner assemblyPlanner )
    {
    assemblyPlanners.add( assemblyPlanner );

    return this;
    }

  /**
   * Method getSources returns the sources of this FlowDef object.
   *
   * @return the sources (type Map<String, Tap>) of this FlowDef object.
   */
  public Map<String, Tap> getSources()
    {
    return sources;
    }

  /**
   * Method getSourcesCopy returns a copy of the sources Map.
   *
   * @return the sourcesCopy (type Map<String, Tap>) of this FlowDef object.
   */
  public Map<String, Tap> getSourcesCopy()
    {
    return new HashMap<String, Tap>( sources );
    }

  /**
   * Method addSource adds a new named source {@link Tap} for use in the resulting {@link Flow}.
   *
   * @param name   of String
   * @param source of Tap
   * @return FlowDef
   */
  public FlowDef addSource( String name, Tap source )
    {
    if( sources.containsKey( name ) )
      throw new IllegalArgumentException( "cannot add duplicate source: " + name );

    sources.put( name, source );
    return this;
    }

  /**
   * Method addSource adds a new source {@link Tap} named after the given {@link Pipe} for use in the resulting {@link Flow}.
   * <p/>
   * If the given pipe is not a head pipe, it will be resolved. If more than one is found, an
   * {@link IllegalArgumentException} will be thrown.
   *
   * @param pipe   of Pipe
   * @param source of Tap
   * @return FlowDef
   */
  public FlowDef addSource( Pipe pipe, Tap source )
    {
    if( pipe == null )
      throw new IllegalArgumentException( "pipe may not be null" );

    Pipe[] heads = pipe.getHeads();

    if( heads.length != 1 )
      throw new IllegalArgumentException( "pipe has too many heads, found: " + Arrays.toString( Pipe.names( heads ) ) );

    addSource( heads[ 0 ].getName(), source );
    return this;
    }

  /**
   * Method addSources adds a map of name and {@link Tap} pairs.
   *
   * @param sources of Map<String, Tap>
   * @return FlowDef
   */
  public FlowDef addSources( Map<String, Tap> sources )
    {
    if( sources != null )
      {
      for( Map.Entry<String, Tap> entry : sources.entrySet() )
        addSource( entry.getKey(), entry.getValue() );
      }

    return this;
    }

  /**
   * Method getSinks returns the sinks of this FlowDef object.
   *
   * @return the sinks (type Map<String, Tap>) of this FlowDef object.
   */
  public Map<String, Tap> getSinks()
    {
    return sinks;
    }

  /**
   * Method getSinksCopy returns a copy of the sink Map.
   *
   * @return the sinksCopy (type Map<String, Tap>) of this FlowDef object.
   */
  public Map<String, Tap> getSinksCopy()
    {
    return new HashMap<String, Tap>( sinks );
    }

  /**
   * Method addSink adds a new named sink {@link Tap} for use in the resulting {@link Flow}.
   *
   * @param name of String
   * @param sink of Tap
   * @return FlowDef
   */
  public FlowDef addSink( String name, Tap sink )
    {
    if( sinks.containsKey( name ) )
      throw new IllegalArgumentException( "cannot add duplicate sink: " + name );

    sinks.put( name, sink );
    return this;
    }

  /**
   * Method addSink adds a new sink {@link Tap} named after the given {@link Pipe} for use in the resulting {@link Flow}.
   *
   * @param tail of Pipe
   * @param sink of Tap
   * @return FlowDef
   */
  public FlowDef addSink( Pipe tail, Tap sink )
    {
    addSink( tail.getName(), sink );
    return this;
    }

  /**
   * Method addTailSink adds the tail {@link Pipe} and sink {@link Tap} to this FlowDef.
   * <p/>
   * This is a convenience method for adding both a tail and sink simultaneously. There isn't a similar method
   * for heads and sources as the head Pipe can always be derived.
   *
   * @param tail of Pipe
   * @param sink of Tap
   * @return FlowDef
   */
  public FlowDef addTailSink( Pipe tail, Tap sink )
    {
    addSink( tail.getName(), sink );
    addTail( tail );
    return this;
    }

  /**
   * Method addSinks adds a Map of the named and {@link Tap} pairs.
   *
   * @param sinks of Map<String, Tap>
   * @return FlowDef
   */
  public FlowDef addSinks( Map<String, Tap> sinks )
    {
    if( sinks != null )
      {
      for( Map.Entry<String, Tap> entry : sinks.entrySet() )
        addSink( entry.getKey(), entry.getValue() );
      }

    return this;
    }

  /**
   * Method getTraps returns the traps of this FlowDef object.
   *
   * @return the traps (type Map<String, Tap>) of this FlowDef object.
   */
  public Map<String, Tap> getTraps()
    {
    return traps;
    }

  /**
   * Method getTrapsCopy returns a copy of the trap Map.
   *
   * @return the trapsCopy (type Map<String, Tap>) of this FlowDef object.
   */
  public Map<String, Tap> getTrapsCopy()
    {
    return new HashMap<String, Tap>( traps );
    }

  /**
   * Method addTrap adds a new named trap {@link Tap} for use in the resulting {@link Flow}.
   *
   * @param name of String
   * @param trap of Tap
   * @return FlowDef
   */
  public FlowDef addTrap( String name, Tap trap )
    {
    if( traps.containsKey( name ) )
      throw new IllegalArgumentException( "cannot add duplicate trap: " + name );

    traps.put( name, trap );
    return this;
    }

  /**
   * Method addTrap adds a new trap {@link Tap} named after the given {@link Pipe} for use in the resulting {@link Flow}.
   *
   * @param pipe of Pipe
   * @param trap of Tap
   * @return FlowDef
   */
  public FlowDef addTrap( Pipe pipe, Tap trap )
    {
    addTrap( pipe.getName(), trap );
    return this;
    }

  /**
   * Method addTraps adds a Map of the names and {@link Tap} pairs.
   *
   * @param traps of Map<String, Tap>
   * @return FlowDef
   */
  public FlowDef addTraps( Map<String, Tap> traps )
    {
    if( traps != null )
      {
      for( Map.Entry<String, Tap> entry : traps.entrySet() )
        addTrap( entry.getKey(), entry.getValue() );
      }

    return this;
    }

  /**
   * Method getCheckpoints returns the checkpoint taps of this FlowDef object.
   *
   * @return the checkpoints (type Map<String, Tap>) of this FlowDef object.
   */
  public Map<String, Tap> getCheckpoints()
    {
    return checkpoints;
    }

  /**
   * Method getCheckpointsCopy returns a copy of the checkpoint tap Map.
   *
   * @return the checkpointsCopy (type Map<String, Tap>) of this FlowDef object.
   */
  public Map<String, Tap> getCheckpointsCopy()
    {
    return new HashMap<String, Tap>( checkpoints );
    }

  /**
   * Method addCheckpoint adds a new named checkpoint {@link Tap} for use in the resulting {@link Flow}.
   *
   * @param name       of String
   * @param checkpoint of Tap
   * @return FlowDef
   */
  public FlowDef addCheckpoint( String name, Tap checkpoint )
    {
    if( checkpoints.containsKey( name ) )
      throw new IllegalArgumentException( "cannot add duplicate checkpoint: " + name );

    checkpoints.put( name, checkpoint );
    return this;
    }

  /**
   * Method addCheckpoint adds a new checkpoint {@link Tap} named after the given {@link Checkpoint} for use in the resulting {@link Flow}.
   *
   * @param pipe       of Pipe
   * @param checkpoint of Tap
   * @return FlowDef
   */
  public FlowDef addCheckpoint( Checkpoint pipe, Tap checkpoint )
    {
    addCheckpoint( pipe.getName(), checkpoint );
    return this;
    }

  /**
   * Method addCheckpoints adds a Map of the names and {@link Tap} pairs.
   *
   * @param checkpoints of Map<String, Tap>
   * @return FlowDef
   */
  public FlowDef addCheckpoints( Map<String, Tap> checkpoints )
    {
    if( checkpoints != null )
      {
      for( Map.Entry<String, Tap> entry : checkpoints.entrySet() )
        addCheckpoint( entry.getKey(), entry.getValue() );
      }

    return this;
    }

  /**
   * Method getTails returns all the current pipe assembly tails the FlowDef holds.
   *
   * @return the tails (type List<Pipe>) of this FlowDef object.
   */
  public List<Pipe> getTails()
    {
    return tails;
    }

  /**
   * Method getTailsArray returns all the current pipe assembly tails the FlowDef holds.
   *
   * @return the tailsArray (type Pipe[]) of this FlowDef object.
   */
  public Pipe[] getTailsArray()
    {
    return tails.toArray( new Pipe[ tails.size() ] );
    }

  /**
   * Method addTail adds a new {@link Pipe} to this FlowDef that represents a tail in a pipe assembly.
   * <p/>
   * Be sure to add a sink tap that has the same name as this tail.
   *
   * @param tail of Pipe
   * @return FlowDef
   */
  public FlowDef addTail( Pipe tail )
    {
    if( tail != null )
      this.tails.add( tail );

    return this;
    }

  /**
   * Method addTails adds a Collection of tails.
   *
   * @param tails of Collection<Pipe>
   * @return FlowDef
   */
  public FlowDef addTails( Collection<Pipe> tails )
    {
    for( Pipe tail : tails )
      addTail( tail );

    return this;
    }

  /**
   * Method addTails adds an array of tails.
   *
   * @param tails of Pipe...
   * @return FlowDef
   */
  public FlowDef addTails( Pipe... tails )
    {
    for( Pipe tail : tails )
      addTail( tail );

    return this;
    }

  public FlowDef setAssertionLevel( AssertionLevel assertionLevel )
    {
    this.assertionLevel = assertionLevel;

    return this;
    }

  public AssertionLevel getAssertionLevel()
    {
    return assertionLevel;
    }

  public FlowDef setDebugLevel( DebugLevel debugLevel )
    {
    this.debugLevel = debugLevel;

    return this;
    }

  public DebugLevel getDebugLevel()
    {
    return debugLevel;
    }

  /**
   * Method setRunID sets the checkpoint run or execution ID to be used to find prior failed runs against
   * this runID.
   * <p/>
   * When given, and a {@link Flow} fails to execute, a subsequent attempt to run the same Flow with the same
   * runID will allow the Flow instance to start where it left off.
   * <p/>
   * Not all planners support this feature.
   * <p/>
   * A Flow name is required when using a runID.
   *
   * @param runID of type String
   * @return FlowDef
   */
  public FlowDef setRunID( String runID )
    {
    if( runID != null && runID.isEmpty() )
      return this;

    this.runID = runID;

    return this;
    }

  public String getRunID()
    {
    return runID;
    }

  public List<String> getClassPath()
    {
    return classPath;
    }

  /**
   * Adds each given artifact to the classpath the assembly will execute under allowing
   * {@link cascading.pipe.Operator}s to dynamically load classes and resources from a {@link ClassLoader}.
   *
   * @param artifact a jar or other file String path
   * @return FlowDef
   */
  public FlowDef addToClassPath( String artifact )
    {
    if( artifact == null || artifact.isEmpty() )
      return this;

    classPath.add( artifact );

    return this;
    }
  }
