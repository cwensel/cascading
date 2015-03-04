/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream.element;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.Splice;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.util.TupleBuilder;

import static cascading.tuple.util.TupleViews.createNarrow;

/**
 *
 */
public abstract class MemorySpliceGate extends GroupingSpliceGate
  {
  protected Set<Tuple> keys;
  protected Map<Tuple, Collection<Tuple>>[] keyValues;

  protected MemoryCoGroupClosure closure;

  protected int numIncomingEventingPaths;

  protected final AtomicInteger count = new AtomicInteger( 0 );

  public MemorySpliceGate( FlowProcess flowProcess, Splice splice )
    {
    super( flowProcess, splice );
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    super.bind( streamGraph );

    numIncomingEventingPaths = streamGraph.findAllPreviousFor( this ).length;
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

    initComparators();

    keys = createKeySet();

    count.set( numIncomingEventingPaths ); // the number of paths incoming
    }

  @Override
  public void prepare()
    {
    super.prepare();

    keyValues = createKeyValuesArray();

    closure = new MemoryCoGroupClosure( flowProcess, splice.getNumSelfJoins(), keyFields, valuesFields );

    if( grouping != null && splice.getJoinDeclaredFields() != null && splice.getJoinDeclaredFields().isNone() )
      grouping.joinerClosure = closure;
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
    Map<Tuple, Collection<Tuple>>[] valueMap = new Map[ getNumDeclaredIncomingBranches() ];

    int start = isBlockingStreamed() ? 0 : 1;
    for( int i = start; i < getNumDeclaredIncomingBranches(); i++ )
      valueMap[ i ] = createTupleMap();

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

  protected abstract boolean isBlockingStreamed();
  }
