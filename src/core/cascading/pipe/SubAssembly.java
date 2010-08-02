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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import cascading.util.Util;

/**
 * Subclasses of SubAssembly encapsulate complex assemblies of {@link Pipe}s so they my be reused in the same manner
 * a Pipe is used.
 * <p/>
 * That is, a typical SubAssembly subclass will accept a 'previous' Pipe instance, and a few
 * arguments for configuring the resulting sub-assembly.
 * <p/>
 * If the SubAssembly represents a split in the pipeline process,
 * all the 'tails' of the assembly must be passed to {@link #setTails(Pipe...)}.
 */
public abstract class SubAssembly extends Pipe
  {
  /** Field assembly */
  private Pipe[] tails;

  /**
   * Must be called by subclasses to set the final end points of the assembly the subclass represents.
   *
   * @param tails of type Pipe
   */
  protected void setTails( Pipe... tails )
    {
    this.tails = tails;
    }

  /**
   * Method getTails returns all the tails of this PipeAssembly object. These values are set by {@link #setTails(Pipe...)}.
   *
   * @return the tails (type Pipe[]) of this PipeAssembly object.
   */
  public Pipe[] getTails()
    {
    return getPrevious(); // just returns a clone of tails
    }

  /**
   * Method getTailNames returns the tailNames of this PipeAssembly object.
   *
   * @return the tailNames (type String[]) of this PipeAssembly object.
   */
  public String[] getTailNames()
    {
    if( tails == null )
      throw new IllegalStateException( Util.formatTrace( this, "setTails must be called in the constructor" ) );

    String[] names = new String[tails.length];

    for( int i = 0; i < tails.length; i++ )
      names[ i ] = tails[ i ].getName();

    return names;
    }

  @Override
  public String getName()
    {
    return Util.join( getTailNames(), "+" );
    }

  @Override
  public Pipe[] getPrevious()
    {
    if( tails == null )
      throw new IllegalStateException( Util.formatTrace( this, "setTails must be called in the constructor" ) );

    return Arrays.copyOf( tails, tails.length );
    }

  /**
   * Is responsible for unwinding nested PipeAssembly instances.
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

    return previous.toArray( new Pipe[previous.size()] );
    }

  }
