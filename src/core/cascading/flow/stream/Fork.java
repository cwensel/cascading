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

package cascading.flow.stream;

/**
 *
 */
public class Fork<Incoming, Outgoing> extends Duct<Incoming, Outgoing>
  {
  protected final Duct[] allNext;

  public Fork( Duct[] allNext )
    {
    this.allNext = allNext;
    }

  @Override
  public Duct getNext()
    {
    // doesn't matter, next after a fork always takes value fields, not grouping fields.
    return allNext[ 0 ];
    }

  @Override
  public void start( Duct previous )
    {
    for( int i = 0; i < allNext.length; i++ )
      allNext[ i ].start( previous );
    }

  @Override
  public void receive( Duct previous, Incoming incoming )
    {
    for( int i = 0; i < allNext.length; i++ )
      allNext[ i ].receive( previous, incoming );
    }

  @Override
  public void complete( Duct previous )
    {
    for( int i = 0; i < allNext.length; i++ )
      allNext[ i ].complete( previous );
    }
  }
