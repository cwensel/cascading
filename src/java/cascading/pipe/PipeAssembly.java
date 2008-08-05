/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import cascading.util.Util;

/**
 * Subclasses of PipeAssembly encapsulate complex assemblies of {@link Pipe}s so they my be reused in the same manner
 * a Pipe is used. That is, a typical PipeAssembly subclass will accept a 'previous' Pipe instance, and a few
 * arguments for configuring the resulting sub-assembly. If the PipeAssembly represents a split in the pipeline process,
 * all the 'tails' of the assembly must be passed to {@link #setTails(Pipe[])}.
 * <p/>
 */
public abstract class PipeAssembly extends Pipe
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
   * Method getTails returns all the tails of this PipeAssembly object. These values are set by {@link #setTails(Pipe[])}.
   *
   * @return the tails (type Pipe[]) of this PipeAssembly object.
   */
  public Pipe[] getTails()
    {
    return tails;
    }

  /**
   * Method getTailNames returns the tailNames of this PipeAssembly object.
   *
   * @return the tailNames (type String[]) of this PipeAssembly object.
   */
  public String[] getTailNames()
    {
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
    return unwindPipeAssemblies( tails );
    }
  }
