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

import cascading.property.ConfigDef;
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
      throw new IllegalStateException( Util.formatRawTrace( this, "setTails must be called in the constructor" ) );

    String[] names = new String[ tails.length ];

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
      throw new IllegalStateException( Util.formatRawTrace( this, "setTails must be called in the constructor" ) );

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

    return previous.toArray( new Pipe[ previous.size() ] );
    }

  @Override
  public ConfigDef getStepConfigDef()
    {
    throw new UnsupportedOperationException( "sub-assembly step config def is unsupported" );
    }

  @Override
  public ConfigDef getConfigDef()
    {
    throw new UnsupportedOperationException( "sub-assembly config def is unsupported" );
    }
  }
