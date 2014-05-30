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

package cascading.flow.hadoop;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.util.TupleBuilder;
import cascading.tuple.util.TupleViews;

/** Class GroupClosure is used internally to represent groups of tuples during grouping. */
public class HadoopGroupByClosure extends JoinerClosure
  {
  protected Tuple grouping;
  protected Iterator[] values;

  public HadoopGroupByClosure( FlowProcess flowProcess, Fields[] groupingFields, Fields[] valueFields )
    {
    super( flowProcess, groupingFields, valueFields );
    }

  public Tuple getGrouping()
    {
    return grouping;
    }

  public int size()
    {
    return 1;
    }

  protected Iterator getValueIterator( int pos )
    {
    return values[ pos ];
    }

  @Override
  public Iterator<Tuple> getIterator( int pos )
    {
    if( pos != 0 )
      throw new IllegalArgumentException( "invalid group position: " + pos );

    return makeIterator( 0, getValueIterator( 0 ) );
    }

  @Override
  public boolean isEmpty( int pos )
    {
    return values != null;
    }

  protected Iterator<Tuple> makeIterator( final int pos, final Iterator values )
    {
    return new Iterator<Tuple>()
    {
    final int cleanPos = valueFields.length == 1 ? 0 : pos; // support repeated pipes
    TupleBuilder[] valueBuilder = new TupleBuilder[ valueFields.length ];

    {
    for( int i = 0; i < valueFields.length; i++ )
      valueBuilder[ i ] = makeBuilder( valueFields[ i ], joinFields[ i ] );
    }

    private TupleBuilder makeBuilder( final Fields valueField, final Fields joinField )
      {
      if( valueField.isUnknown() || joinField.isNone() )
        return new TupleBuilder()
        {
        @Override
        public Tuple makeResult( Tuple valueTuple, Tuple groupTuple )
          {
          valueTuple.set( valueFields[ cleanPos ], joinFields[ cleanPos ], groupTuple );

          return valueTuple;
          }
        };

      return new TupleBuilder()
      {
      Tuple result = TupleViews.createOverride( valueField, joinField );

      @Override
      public Tuple makeResult( Tuple valueTuple, Tuple groupTuple )
        {
        return TupleViews.reset( result, valueTuple, groupTuple );
        }
      };
      }

    public boolean hasNext()
      {
      return values.hasNext();
      }

    public Tuple next()
      {
      Tuple tuple = (Tuple) values.next();

      return valueBuilder[ cleanPos ].makeResult( tuple, grouping );
      }

    public void remove()
      {
      throw new UnsupportedOperationException( "remove not supported" );
      }
    };
    }

  public void reset( Tuple grouping, Iterator<Tuple>... values )
    {
    this.grouping = grouping;
    this.values = values;
    }

  @Override
  public Tuple getGroupTuple( Tuple keysTuple )
    {
    return keysTuple;
    }
  }
