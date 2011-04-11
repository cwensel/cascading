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

import java.util.Iterator;

/**
 *
 */
public class OpenReducingDuct<Incoming, Outgoing> extends Duct<Grouping<Incoming, Iterator<Incoming>>, Outgoing> implements OpenWindow
  {
  final Reducing reducing;

  public OpenReducingDuct( Duct<Outgoing, ?> next )
    {
    super( next );

    reducing = (Reducing) getNext();
    }

  @Override
  public void receive( Duct previous, Grouping<Incoming, Iterator<Incoming>> grouping )
    {
    // don't start a grouping if there are no values in the group
    if( !grouping.iterator.hasNext() )
      return;

    reducing.startGroup( previous, grouping.group );

    while( grouping.iterator.hasNext() )
      next.receive( this, (Outgoing) grouping.iterator.next() );

    reducing.completeGroup( previous, grouping.group );
    }
  }
