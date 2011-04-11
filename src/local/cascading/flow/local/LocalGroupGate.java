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

package cascading.flow.local;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.flow.stream.GroupGate;
import cascading.pipe.Group;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
 */
public abstract class LocalGroupGate extends GroupGate
  {
  protected Set<Tuple> keysSet;

  protected Comparator<Tuple>[] groupComparators;
  protected Comparator<Tuple>[] valueComparators;

  protected final AtomicInteger count = new AtomicInteger( 0 );

  public LocalGroupGate( FlowProcess flowProcess, Group group )
    {
    super( flowProcess, group );
    }

  @Override
  public void initialize()
    {
    super.initialize();

    orderDucts();

    groupComparators = new Comparator[ orderedPrevious.length ];

    if( group.isSorted() )
      valueComparators = new Comparator[ orderedPrevious.length ];

    for( Scope incomingScope : incomingScopes )
      {
      int pos = group.getPipePos().get( incomingScope.getName() );

      // we want the comparators
      Fields groupFields = group.getGroupingSelectors().get( incomingScope.getName() );

      groupComparators[ pos ] = group.isSortReversed() ? Collections.reverseOrder( groupFields ) : groupFields;

      if( sortFields != null )
        {
        // we want the comparators
        Fields sortFields = group.getSortingSelectors().get( incomingScope.getName() );
        valueComparators[ pos ] = new SparseTupleComparator( valuesFields[ pos ], sortFields );

        if( group.isSortReversed() )
          valueComparators[ pos ] = Collections.reverseOrder( valueComparators[ pos ] );
        }
      }

    count.set( allPrevious.length ); // the actual value

    keysSet = initNewKeySet();
    }

  protected Set<Tuple> initNewKeySet()
    {
    // only sorting groupby currently
    return Collections.synchronizedSet( new TreeSet<Tuple>( groupComparators[ 0 ] ) );
    }
  }
