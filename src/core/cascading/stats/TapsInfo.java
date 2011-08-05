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

package cascading.stats;

import java.util.ArrayList;
import java.util.Collection;

import cascading.tap.CompositeTap;
import cascading.tap.Tap;

/**
 *
 */
public class TapsInfo
  {
  private Collection<Tap> taps;

  public TapsInfo( Collection<Tap> taps )
    {
    this.taps = taps;
    }

  public Collection<Tap> getParentTaps()
    {
    return new ArrayList<Tap>( taps );
    }

  public Collection<Tap> getChildTaps()
    {
    ArrayList<Tap> children = new ArrayList<Tap>();

    for( Tap tap : taps )
      addTap( children, tap );

    return children;
    }

  private void addTap( ArrayList<Tap> children, Tap tap )
    {
    if( tap instanceof CompositeTap )
      {
      for( Tap child : children )
        addTap( children, child );
      }
    else
      {
      children.add( tap );
      }
    }
  }
