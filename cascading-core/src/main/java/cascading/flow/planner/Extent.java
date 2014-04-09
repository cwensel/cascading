/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner;

import java.util.Set;

import cascading.pipe.Pipe;

/**
 *
 */
public class Extent extends Pipe
  {
  /** Field head */
  public static final Extent head = new Extent( "head" );
  /** Field tail */
  public static final Extent tail = new Extent( "tail" );

  private Extent( String name )
    {
    super( name );
    }

  @Override
  public Scope outgoingScopeFor( Set<Scope> scopes )
    {
    return new Scope();
    }

  @Override
  public String toString()
    {
    return "[" + getName() + "]";
    }

  @Override
  public boolean equals( Object object )
    {
    if( object == null )
      return false;

    if( this == object )
      return true;

    if( object.getClass() != this.getClass() )
      return false;

    return this.getName().equals( ( (Pipe) object ).getName() );
    }
  }
