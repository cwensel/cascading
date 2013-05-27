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

package cascading.pipe;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.property.ConfigDef;
import cascading.tuple.Fields;
import cascading.util.Util;

import static java.util.Arrays.asList;

/**
 * Class Pipe is used to name branches in pipe assemblies, and as a base class for core
 * processing model types, specifically {@link Each}, {@link Every}, {@link GroupBy},
 * {@link CoGroup}, {@link Merge}, {@link HashJoin}, and {@link SubAssembly}.
 * <p/>
 * Pipes are chained together through their constructors.
 * <p/>
 * To effect a split in the pipe,
 * simply pass a Pipe instance to two or more constructors of subsequent Pipe instances.
 * </p>
 * A join can be achieved by passing two or more Pipe instances to a {@link CoGroup} or {@link HashJoin} pipe.
 * <p/>
 * A merge can be achieved by passing two or more Pipe instances to a {@link GroupBy} or {@link Merge} pipe.
 *
 * @see Each
 * @see Every
 * @see GroupBy
 * @see Merge
 * @see CoGroup
 * @see HashJoin
 * @see SubAssembly
 */
public class Pipe implements FlowElement, Serializable
  {
  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;
  /** Field name */
  protected String name;
  /** Field previous */
  protected Pipe previous;
  /** Field parent */
  protected Pipe parent;

  protected ConfigDef configDef;

  protected ConfigDef stepConfigDef;

  /** Field id */
  private String id;
  /** Field trace */
  private final String trace = Util.captureDebugTrace( getClass() );

  public static synchronized String id( Pipe pipe )
    {
    if( pipe.id == null )
      pipe.id = Util.createUniqueID();

    return pipe.id;
    }

  /**
   * Convenience method to create an array of Pipe instances.
   *
   * @param pipes vararg list of pipes
   * @return array of pipes
   */
  public static Pipe[] pipes( Pipe... pipes )
    {
    return pipes;
    }

  /**
   * Convenience method for finding all Pipe names in an assembly.
   *
   * @param tails vararg list of all tails in given assembly
   * @return array of Pipe names
   */
  public static String[] names( Pipe... tails )
    {
    Set<String> names = new HashSet<String>();

    collectNames( tails, names );

    return names.toArray( new String[ names.size() ] );
    }

  private static void collectNames( Pipe[] pipes, Set<String> names )
    {
    for( Pipe pipe : pipes )
      {
      if( pipe instanceof SubAssembly )
        names.addAll( asList( ( (SubAssembly) pipe ).getTailNames() ) );
      else
        names.add( pipe.getName() );

      collectNames( SubAssembly.unwind( pipe.getPrevious() ), names );
      }
    }

  public static Pipe[] named( String name, Pipe... tails )
    {
    Set<Pipe> pipes = new HashSet<Pipe>();

    collectPipes( name, tails, pipes );

    return pipes.toArray( new Pipe[ pipes.size() ] );
    }

  private static void collectPipes( String name, Pipe[] tails, Set<Pipe> pipes )
    {
    for( Pipe tail : tails )
      {
      if( !( tail instanceof SubAssembly ) && tail.getName().equals( name ) )
        pipes.add( tail );

      collectPipes( name, SubAssembly.unwind( tail.getPrevious() ), pipes );
      }
    }

  static Pipe[] resolvePreviousAll( Pipe... pipes )
    {
    Pipe[] resolved = new Pipe[ pipes.length ];

    for( int i = 0; i < pipes.length; i++ )
      resolved[ i ] = resolvePrevious( pipes[ i ] );

    return resolved;
    }

  static Pipe resolvePrevious( Pipe pipe )
    {
    if( pipe instanceof Splice || pipe instanceof Operator )
      return pipe;

    Pipe[] pipes = pipe.getPrevious();

    if( pipes.length > 1 )
      throw new IllegalStateException( "cannot resolve SubAssemblies with multiple tails at this time" );

    for( Pipe previous : pipes )
      {
      if( previous instanceof Splice || previous instanceof Operator )
        return previous;

      return resolvePrevious( previous );
      }

    return pipe;
    }

  protected Pipe()
    {
    }

  @ConstructorProperties({"previous"})
  protected Pipe( Pipe previous )
    {
    this.previous = previous;

    verifyPipe();
    }

  /**
   * Constructor Pipe creates a new Pipe instance with the given name. This is useful as the 'start' or head
   * of a pipe assembly.
   *
   * @param name name for this branch of Pipes
   */
  @ConstructorProperties({"name"})
  public Pipe( String name )
    {
    this.name = name;
    }

  /**
   * Constructor Pipe creates a new Pipe instance with the given name and previous Pipe instance. This is useful for
   * naming a branch in a pipe assembly. Or renaming the branch mid-way down.
   *
   * @param name     name for this branch of Pipes
   * @param previous previous Pipe to receive input Tuples from
   */
  @ConstructorProperties({"name", "previous"})
  public Pipe( String name, Pipe previous )
    {
    this.name = name;
    this.previous = previous;

    verifyPipe();
    }

  private void verifyPipe()
    {
    if( !( previous instanceof SubAssembly ) )
      return;

    String[] strings = ( (SubAssembly) previous ).getTailNames();
    if( strings.length != 1 )
      throw new IllegalArgumentException( "pipe assembly must not return more than one tail pipe instance, found " + Util.join( strings, ", " ) );
    }

  /**
   * Get the name of this pipe. Guaranteed non-null.
   *
   * @return String the name of this pipe
   */
  public String getName()
    {
    if( name != null )
      return name;

    if( previous != null )
      return previous.getName();

    return "ANONYMOUS";
    }

  /**
   * Get all the upstream pipes this pipe is connected to. This method will return the Pipe instances
   * passed on the constructors as inputs to this Pipe instance.
   *
   * @return all the upstream pipes this pipe is connected to.
   */
  public Pipe[] getPrevious()
    {
    if( previous == null )
      return new Pipe[ 0 ];

    return new Pipe[]{previous};
    }

  protected void setParent( Pipe parent )
    {
    this.parent = parent;
    }

  /**
   * Returns the enclosing parent Pipe instance, if any. A parent is typically a {@link SubAssembly} that wraps
   * this instance.
   *
   * @return of type Pipe
   */
  public Pipe getParent()
    {
    return parent;
    }

  /**
   * Returns a {@link ConfigDef} instance that allows for local properties to be set and made available via
   * a resulting {@link cascading.flow.FlowProcess} instance when the pipe is invoked.
   * <p/>
   * Any properties set on the configDef will not show up in any {@link cascading.flow.Flow} or
   * {@link cascading.flow.FlowStep} process level configuration, but will override any of those values as seen by the
   * current Pipe instance.
   *
   * @return an instance of ConfigDef
   */
  @Override
  public ConfigDef getConfigDef()
    {
    if( configDef == null )
      configDef = new ConfigDef();

    return configDef;
    }

  /**
   * Returns {@code true} if there are properties in the configDef instance.
   *
   * @return true if there are configDef properties
   */
  @Override
  public boolean hasConfigDef()
    {
    return configDef != null && !configDef.isEmpty();
    }

  /**
   * Returns a {@link ConfigDef} instance that allows for process level properties to be set and made available via
   * a resulting {@link cascading.flow.FlowProcess} instance when the pipe is invoked.
   * <p/>
   * Any properties set on the stepConfigDef will not show up in any Flow configuration, but will show up in
   * the current process {@link cascading.flow.FlowStep} (in Hadoop the MapReduce jobconf). Any value set in the
   * stepConfigDef will be overridden by the pipe local {@code #getConfigDef} instance.
   * </p>
   * Use this method to tweak properties in the process step this pipe instance is planned into. In the case of the
   * Hadoop platform, when set on a {@link GroupBy} instance, the number of reducers can be modified.
   *
   * @return an instance of ConfigDef
   */
  @Override
  public ConfigDef getStepConfigDef()
    {
    if( stepConfigDef == null )
      stepConfigDef = new ConfigDef();

    return stepConfigDef;
    }

  /**
   * Returns {@code true} if there are properties in the processConfigDef instance.
   *
   * @return true if there are processConfigDef properties
   */
  @Override
  public boolean hasStepConfigDef()
    {
    return stepConfigDef != null && !stepConfigDef.isEmpty();
    }

  /**
   * Method getHeads returns the first Pipe instances in this pipe assembly.
   *
   * @return the first (type Pipe[]) of this Pipe object.
   */
  public Pipe[] getHeads()
    {
    Pipe[] pipes = getPrevious();

    if( pipes.length == 0 )
      return new Pipe[]{this};

    if( pipes.length == 1 )
      return pipes[ 0 ].getHeads();

    Set<Pipe> heads = new HashSet<Pipe>();

    for( Pipe pipe : pipes )
      Collections.addAll( heads, pipe.getHeads() );

    return heads.toArray( new Pipe[ heads.size() ] );
    }

  @Override
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    return incomingScopes.iterator().next();
    }

  @Override
  public Fields resolveIncomingOperationArgumentFields( Scope incomingScope )
    {
    throw new IllegalStateException( "resolveIncomingOperationFields should never be called" );
    }

  @Override
  public Fields resolveIncomingOperationPassThroughFields( Scope incomingScope )
    {
    throw new IllegalStateException( "resolveIncomingOperationPassThroughFields should never be called" );
    }

  /**
   * Method getTrace returns a String that pinpoint where this instance was created for debugging.
   *
   * @return String
   */
  public String getTrace()
    {
    return trace;
    }

  @Override
  public String toString()
    {
    return getClass().getSimpleName() + "(" + getName() + ")";
    }

  Scope getFirst( Set<Scope> incomingScopes )
    {
    return incomingScopes.iterator().next();
    }

  @Override
  public boolean isEquivalentTo( FlowElement element )
    {
    if( element == null )
      return false;

    if( this == element )
      return true;

    return getClass() == element.getClass();
    }

  @SuppressWarnings({"EqualsWhichDoesntCheckParameterClass"})
  @Override
  public boolean equals( Object object )
    {
    // we cannot test equality by names for this class, prevents detection of dupe names in heads or tails
    return this == object;
    }

  @Override
  public int hashCode()
    {
    return 31 * getName().hashCode() + getClass().hashCode();
    }

  /**
   * Method print is used internally.
   *
   * @param scope of type Scope
   * @return String
   */
  public String print( Scope scope )
    {
    StringBuffer buffer = new StringBuffer();

    printInternal( buffer, scope );

    return buffer.toString();
    }

  protected void printInternal( StringBuffer buffer, Scope scope )
    {
    buffer.append( getClass().getSimpleName() ).append( "('" ).append( getName() ).append( "')" );
    }
  }
