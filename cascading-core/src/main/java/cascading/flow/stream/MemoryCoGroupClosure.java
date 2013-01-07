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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;
import cascading.tuple.util.TupleViews;

/**
 *
 */
public class MemoryCoGroupClosure extends JoinerClosure
  {
  private Collection<Tuple>[] collections;
  private final int numSelfJoins;
  private final Tuple emptyTuple;
  private Tuple joinedTuple = new Tuple(); // is discarded

  private Tuple[] joinedTuplesArray;
  private TupleBuilder joinedBuilder;

  public MemoryCoGroupClosure( FlowProcess flowProcess, int numSelfJoins, Fields[] groupingFields, Fields[] valueFields )
    {
    super( flowProcess, groupingFields, valueFields );
    this.numSelfJoins = numSelfJoins;
    this.emptyTuple = Tuple.size( groupingFields[ 0 ].size() );

    this.joinedTuplesArray = new Tuple[ size() ];
    this.joinedBuilder = makeJoinedBuilder( groupingFields );
    }

  @Override
  public int size()
    {
    return Math.max( joinFields.length, numSelfJoins + 1 );
    }

  public void reset( Collection<Tuple>[] collections )
    {
    this.collections = collections;
    }

  @Override
  public Iterator<Tuple> getIterator( int pos )
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

  @Override
  public Tuple getGroupTuple( Tuple keysTuple )
    {
    Tuples.asModifiable( joinedTuple );

    for( int i = 0; i < collections.length; i++ )
      joinedTuplesArray[ i ] = collections[ i ].isEmpty() ? emptyTuple : keysTuple;

    joinedTuple = joinedBuilder.makeResult( joinedTuplesArray );

    return joinedTuple;
    }

  static interface TupleBuilder
    {
    Tuple makeResult( Tuple[] tuples );
    }

  private TupleBuilder makeJoinedBuilder( final Fields[] joinFields )
    {
    final Fields[] fields = isSelfJoin() ? new Fields[ size() ] : joinFields;

    if( isSelfJoin() )
      Arrays.fill( fields, 0, fields.length, joinFields[ 0 ] );

    return new TupleBuilder()
    {
    Tuple result = TupleViews.createComposite( fields );

    @Override
    public Tuple makeResult( Tuple[] tuples )
      {
      return TupleViews.reset( result, tuples );
      }
    };
    }
  }
