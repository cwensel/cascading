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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subclasses of SubAssembly encapsulate complex assemblies of {@link Pipe}s so they my be reused in the same manner
 * a Pipe is used.
 * <p/>
 * That is, a typical SubAssembly subclass will accept a 'previous' Pipe instance, and a few
 * arguments for configuring the resulting sub-assembly.
 * <p/>
 * The previous pipe (or pipes) must be passed on the super constructor, or set via {@link #setPrevious(Pipe...)}. This
 * allows the current SubAssembly to become the parent of any Pipe instances between the previous and the tails,
 * exclusive of the previous, and inclusive of the tails.
 * <p/>
 * Subsequently all tail Pipes must be set via the {@link #setTails(Pipe...)} method.
 * <p/>
 * Note if the SubAssembly represents a split in the pipeline process,
 * all the 'tails' of the assembly must be passed to {@link #setTails(Pipe...)}. It is up the the developer to
 * provide any other access to the tails so they may be chained into any subsequent Pipes.
 * <p/>
 * Any {@link cascading.property.ConfigDef} values on this SubAssembly will be honored by child Pipe instances via the
 * {@link cascading.pipe.Pipe#getParent()} back link described above.
 */
public abstract class SubAssembly extends Pipe
  {
  private static final Logger LOG = LoggerFactory.getLogger( SubAssembly.class );

  /** Field previous */
  private Pipe[] previous; // actual previous pipes instances
  /** Field tails */
  private Pipe[] tails;

  /**
   * Is responsible for unwinding nested SubAssembly instances.
   *
   * @param tails of type Pipe[]
   * @return a Pipe[]
   */
  public static Pipe[] unwind( Pipe... tails )
    {
    Set<Pipe> previous = new HashSet<Pipe>();

    for( Pipe pipe : tails )
      {
      if( pipe instanceof SubAssembly )
        Collections.addAll( previous, unwind( pipe.getPrevious() ) );
      else
        previous.add( pipe );
      }

    return previous.toArray( new Pipe[ previous.size() ] );
    }

  protected SubAssembly()
    {
    }

  protected SubAssembly( Pipe... previous )
    {
    setPrevious( previous );
    }

  protected SubAssembly( String name, Pipe[] previous )
    {
    super( name );
    setPrevious( previous );
    }

  /**
   * Must be called by subclasses to set the start end points of the assembly the subclass represents.
   *
   * @param previous of type Pipe
   */
  protected void setPrevious( Pipe... previous )
    {
    this.previous = previous;
    }

  /**
   * Must be called by subclasses to set the final end points of the assembly the subclass represents.
   *
   * @param tails of type Pipe
   */
  protected void setTails( Pipe... tails )
    {
    this.tails = tails;

    if( previous == null )
      {
      LOG.warn( "previous pipes not set via setPrevious or constructor on: {}", this );
      return;
      }

    Set<Pipe> stopSet = new HashSet<Pipe>();

    Collections.addAll( stopSet, previous );

    setParent( stopSet, tails );
    }

  private void setParent( Set<Pipe> stopSet, Pipe[] tails )
    {
    if( tails == null )
      return;

    for( Pipe tail : tails )
      {
      if( stopSet.contains( tail ) )
        continue;

      tail.setParent( this );

      Pipe[] current;

      if( tail instanceof SubAssembly )
        current = ( (SubAssembly) tail ).previous;
      else
        current = tail.getPrevious();

      if( current == null && tail instanceof SubAssembly )
        LOG.warn( "previous pipes not set via setPrevious or constructor on: {}", tail );

      setParent( stopSet, current );
      }
    }

  /**
   * Method getTails returns all the tails of this SubAssembly object. These values are set by {@link #setTails(Pipe...)}.
   *
   * @return the tails (type Pipe[]) of this SubAssembly object.
   */
  public Pipe[] getTails()
    {
    return getPrevious(); // just returns a clone of tails
    }

  /**
   * Method getTailNames returns the tailNames of this SubAssembly object.
   *
   * @return the tailNames (type String[]) of this SubAssembly object.
   */
  public String[] getTailNames()
    {
    if( tails == null )
      throw new IllegalStateException( Util.formatRawTrace( this, "setTails must be called in the constructor" ) );

    String[] names = new String[ tails.length ];

    for( int i = 0; i < tails.length; i++ )
      names[ i ] = tails[ i ].getName();

    return names;
    }

  @Override
  public String getName()
    {
    if( name != null )
      return name;

    return Util.join( getTailNames(), "+" );
    }

  @Override
  public Pipe[] getPrevious()
    {
    // returns the semantically equivalent to Pipe#previous to simplify logic in the planner
    // SubAssemblies are really aliases for their tails
    if( tails == null )
      throw new IllegalStateException( Util.formatRawTrace( this, "setTails must be called after the sub-assembly is assembled" ) );

    return Arrays.copyOf( tails, tails.length );
    }
  }
