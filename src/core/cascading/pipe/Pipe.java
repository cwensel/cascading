/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.Scope;
import cascading.tuple.Fields;
import cascading.util.Util;

/**
 * Class Pipe is used to name branches in pipe assemblies, and as a base class for core
 * processing model types, specifically {@link Each}, {@link Every}, {@link GroupBy}, and
 * {@link CoGroup}.
 * <p/>
 * Pipes are chained together through their constructors.
 * <p/>
 * To effect a split in the pipe,
 * simply pass a Pipe instance to two or more constructors of subsequent Pipe instances.
 * </p>
 * A join can be achieved by passing two or more Pipe instances to a {@link CoGroup} pipe.
 * <p/>
 * A merge can be achieved by passing two or more Pipe instances to a {@link GroupBy} pipe.
 *
 * @see Each
 * @see Every
 * @see GroupBy
 * @see CoGroup
 */
public class Pipe implements FlowElement, Serializable
  {
  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;
  /** Field name */
  private String name;
  /** Field previous */
  protected Pipe previous;
  /** Field trace */
  private String trace = Util.captureDebugTrace( getClass() );

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

    return names.toArray( new String[names.size()] );
    }

  private static void collectNames( Pipe[] pipes, Set<String> names )
    {
    for( Pipe pipe : pipes )
      {
      if( pipe instanceof SubAssembly )
        names.addAll( Arrays.asList( ( (SubAssembly) pipe ).getTailNames() ) );
      else
        names.add( pipe.getName() );

      collectNames( SubAssembly.unwind( pipe.getPrevious() ), names );
      }
    }

  static Pipe[] resolvePreviousAll( Pipe... pipes )
    {
    Pipe[] resolved = new Pipe[pipes.length];

    for( int i = 0; i < pipes.length; i++ )
      resolved[ i ] = resolvePrevious( pipes[ i ] );

    return resolved;
    }

  static Pipe resolvePrevious( Pipe pipe )
    {
    Pipe[] pipes = pipe.getPrevious();

    if( pipes.length != 1 )
      throw new IllegalStateException( "cannot resolve SubAssemblies with multiple tails at this time" );

    for( Pipe previous : pipes )
      {
      if( previous instanceof Group || previous instanceof Operator )
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
      return new Pipe[0];

    return new Pipe[]{previous};
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

    return heads.toArray( new Pipe[heads.size()] );
    }

  /** @see FlowElement#outgoingScopeFor(Set<Scope>) */
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    return incomingScopes.iterator().next();
    }

  /** @see FlowElement#resolveIncomingOperationFields(Scope) */
  public Fields resolveIncomingOperationFields( Scope incomingScope )
    {
    throw new IllegalStateException( "resolveIncomingOperationFields should never be called" );
    }

  /** @see FlowElement#resolveFields(Scope) */
  public Fields resolveFields( Scope scope )
    {
    throw new IllegalStateException( "resolveFields should never be called" );
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
