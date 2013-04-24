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

package cascading.flow.stream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import cascading.flow.FlowProcess;
import cascading.flow.FlowProps;
import cascading.flow.planner.Scope;
import cascading.pipe.Splice;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.util.TupleBuilder;
import cascading.tuple.util.TupleHasher;

import static cascading.tuple.util.TupleViews.createNarrow;

/**
 *
 */
public abstract class MemorySpliceGate extends SpliceGate
  {

  protected final Map<Duct, Integer> posMap = new IdentityHashMap<Duct, Integer>();

  protected Comparator<Tuple>[] groupComparators;
  protected Comparator<Tuple>[] valueComparators;
  protected TupleHasher groupHasher;

  protected Set<Tuple> keys;
  protected Map<Tuple, Collection<Tuple>>[] keyValues;

  protected MemoryCoGroupClosure closure;

  protected int numIncomingPaths;

  protected final AtomicInteger count = new AtomicInteger( 0 );

  public MemorySpliceGate( FlowProcess flowProcess, Splice splice )
    {
    super( flowProcess, splice );
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    super.bind( streamGraph ); // finds allPrevious

    numIncomingPaths = streamGraph.countAllEventingPathsTo( this );

    orderDucts( streamGraph );
    }

  // we must make a new Tuple instance to wrap the incoming copy
  protected TupleBuilder createDefaultNarrowBuilder( final Fields incomingFields, final Fields narrowFields )
    {
    return new TupleBuilder()
    {
    int[] pos = incomingFields.getPos( narrowFields );

    @Override
    public Tuple makeResult( Tuple input, Tuple output )
      {
      return createNarrow( pos, input );
      }
    };
    }

  @Override
  public void initialize()
    {
    super.initialize();

    Comparator defaultComparator = (Comparator) flowProcess.newInstance( (String) flowProcess.getProperty( FlowProps.DEFAULT_ELEMENT_COMPARATOR ) );

    Fields[] compareFields = new Fields[ orderedPrevious.length ];
    groupComparators = new Comparator[ orderedPrevious.length ];

    if( splice.isSorted() )
      valueComparators = new Comparator[ orderedPrevious.length ];

    int size = splice.isGroupBy() ? 1 : incomingScopes.size();

    for( int i = 0; i < size; i++ )
      {
      Scope incomingScope = incomingScopes.get( i );

      int pos = splice.isGroupBy() ? 0 : splice.getPipePos().get( incomingScope.getName() );

      // we want the comparators
      Fields groupFields = splice.getKeySelectors().get( incomingScope.getName() );

      compareFields[ pos ] = groupFields; // used for finding hashers

      if( groupFields.size() == 0 )
        groupComparators[ pos ] = groupFields;
      else
        groupComparators[ pos ] = new SparseTupleComparator( Fields.asDeclaration( groupFields ), defaultComparator );

      groupComparators[ pos ] = splice.isSortReversed() ? Collections.reverseOrder( groupComparators[ pos ] ) : groupComparators[ pos ];

      if( sortFields != null )
        {
        // we want the comparators, so don't use sortFields array
        Fields sortFields = splice.getSortingSelectors().get( incomingScope.getName() );
        valueComparators[ pos ] = new SparseTupleComparator( valuesFields[ pos ], sortFields, defaultComparator );

        if( splice.isSortReversed() )
          valueComparators[ pos ] = Collections.reverseOrder( valueComparators[ pos ] );
        }
      }

    Comparator[] hashers = TupleHasher.merge( compareFields );
    groupHasher = defaultComparator != null || !TupleHasher.isNull( hashers ) ? new TupleHasher( defaultComparator, hashers ) : null;

    keys = createKeySet();

    count.set( numIncomingPaths ); // the number of paths incoming
    }

  @Override
  public void prepare()
    {
    super.prepare();

    keyValues = createKeyValuesArray();

    makePosMap( posMap );

    closure = new MemoryCoGroupClosure( flowProcess, splice.getNumSelfJoins(), keyFields, valuesFields );
    }

  protected Comparator getKeyComparator()
    {
    if( groupComparators.length > 0 && groupComparators[ 0 ] != null )
      return groupComparators[ 0 ];

    return new Comparator<Comparable>()
    {
    @Override
    public int compare( Comparable lhs, Comparable rhs )
      {
      return lhs.compareTo( rhs );
      }
    };
    }

  protected Set<Tuple> createKeySet()
    {
    return Collections.synchronizedSet( new TreeSet<Tuple>( getKeyComparator() ) );
    }

  /**
   * This lets us just replace an old map and let the gc cleanup, vs clearing each map
   *
   * @return of type Map
   */
  protected Map<Tuple, Collection<Tuple>>[] createKeyValuesArray()
    {
    // Ducts use identity for equality
    Map<Tuple, Collection<Tuple>>[] valueMap = new Map[ orderedPrevious.length ];

    int start = isBlockingStreamed() ? 0 : 1;
    for( int i = start; i < orderedPrevious.length; i++ )
      {
      if( orderedPrevious[ i ] == null ) // only true for local mode
        continue;

      valueMap[ i ] = createTupleMap();
      }

    return valueMap;
    }

  protected Map<Tuple, Collection<Tuple>> createTupleMap()
    {
    return new HashMap<Tuple, Collection<Tuple>>()
    {
    @Override
    public Collection<Tuple> get( Object object )
      {
      Collection<Tuple> value = super.get( object );

      if( value == null )
        {
        value = new ArrayList<Tuple>();

        super.put( (Tuple) object, value );
        }

      return value;
      }
    };
    }

  /**
   * This allows the tuple to honor the hasher and comparators, if any
   *
   * @param object the tuple to wrap
   * @return a DelegatedTuple instance
   */
  protected final Tuple getDelegatedTuple( Tuple object )
    {
    if( groupHasher == null )
      return object;

    return new DelegatedTuple( object );
    }

  protected abstract boolean isBlockingStreamed();

  protected class DelegatedTuple extends Tuple
    {
    public DelegatedTuple( Tuple wrapped )
      {
      // pass it in to prevent one being allocated
      super( Tuple.elements( wrapped ) );
      }

    @Override
    public boolean equals( Object object )
      {
      return compareTo( object ) == 0;
      }

    @Override
    public int compareTo( Object other )
      {
      return groupComparators[ 0 ].compare( this, (Tuple) other );
      }

    @Override
    public int hashCode()
      {
      return groupHasher.hashCode( this );
      }
    }
  }
