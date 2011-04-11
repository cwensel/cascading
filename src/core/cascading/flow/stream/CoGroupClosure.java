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

import java.util.Collection;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.pipe.cogroup.GroupClosure;
import cascading.tuple.Fields;

/**
 *
 */
public class CoGroupClosure extends GroupClosure
  {
  private Collection[] collections;
  private final int numSelfJoins;

  public CoGroupClosure( FlowProcess flowProcess, int numSelfJoins, Fields[] groupingFields, Fields[] valueFields )
    {
    super( flowProcess, groupingFields, valueFields );
    this.numSelfJoins = numSelfJoins;
    }

  @Override
  public int size()
    {
    return Math.max( groupingFields.length, numSelfJoins + 1 );
    }

  public void reset( Collection[] collections )
    {
    this.collections = collections;
    }

  @Override
  public Iterator getIterator( int pos )
    {
    if( numSelfJoins != 0 )
      return collections[ 0 ].iterator();
    else
      return collections[ pos ].iterator();
    }

  @Override
  public boolean isEmpty( int pos )
    {
    if( numSelfJoins != 0 )
      return collections[ 0 ].isEmpty();
    else
      return collections[ pos ].isEmpty();
    }
  }
