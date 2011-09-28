/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
