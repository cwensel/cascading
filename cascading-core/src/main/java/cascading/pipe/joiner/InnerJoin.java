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

package cascading.pipe.joiner;

import java.util.Arrays;
import java.util.Iterator;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;
import cascading.tuple.util.TupleViews;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class InnerJoin will return an {@link Iterator} that will iterate over a given {@link Joiner} and return tuples that represent
 * and inner join of the CoGrouper internal grouped tuple collections.
 * <p/>
 * Joins perform based on the equality of the join keys. In the case of null values, Java treats two
 * null values as equivalent. SQL does not treat null values as equal. To produce SQL like results in a given
 * join, a new {@link java.util.Comparator} will need to be used on the joined values to prevent null from
 * equaling null. As a convenience, see the {@link cascading.util.NullNotEquivalentComparator} class.
 */
public class InnerJoin implements Joiner
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( InnerJoin.class );

  public Iterator<Tuple> getIterator( JoinerClosure closure )
    {
    return new JoinIterator( closure );
    }

  public int numJoins()
    {
    return -1;
    }

  public static class JoinIterator implements Iterator<Tuple>
    {
    final JoinerClosure closure;
    Iterator[] iterators;
    Tuple[] lastValues;

    TupleBuilder resultBuilder;
    Tuple result = new Tuple(); // will be replaced

    public JoinIterator( JoinerClosure closure )
      {
      this.closure = closure;

      LOG.debug( "cogrouped size: {}", closure.size() );

      init();
      }

    protected void init()
      {
      iterators = new Iterator[ closure.size() ];

      for( int i = 0; i < closure.size(); i++ )
        iterators[ i ] = getIterator( i );

      boolean isUnknown = false;

      for( Fields fields : closure.getValueFields() )
        isUnknown |= fields.isUnknown();

      if( isUnknown )
        resultBuilder = new TupleBuilder()
        {
        Tuple result = new Tuple(); // is re-used

        @Override
        public Tuple makeResult( Tuple[] tuples )
          {
          result.clear();

          // flatten the results into one Tuple
          for( Tuple lastValue : tuples )
            result.addAll( lastValue );

          return result;
          }
        };
      else
        resultBuilder = new TupleBuilder()
        {
        Tuple result;

        {
        // handle self join.
        Fields[] fields = closure.getValueFields();

        if( closure.isSelfJoin() )
          {
          fields = new Fields[ closure.size() ];

          Arrays.fill( fields, closure.getValueFields()[ 0 ] );
          }

        result = TupleViews.createComposite( fields );
        }

        @Override
        public Tuple makeResult( Tuple[] tuples )
          {
          return TupleViews.reset( result, tuples );
          }
        };
      }

    protected Iterator getIterator( int i )
      {
      return closure.getIterator( i );
      }

    private Tuple[] initLastValues()
      {
      lastValues = new Tuple[ iterators.length ];

      for( int i = 0; i < iterators.length; i++ )
        lastValues[ i ] = (Tuple) iterators[ i ].next();

      return lastValues;
      }

    public final boolean hasNext()
      {
      // if this is the first pass, and there is an iterator without a next value,
      // then we have no next element
      if( lastValues == null )
        {
        for( Iterator iterator : iterators )
          {
          if( !iterator.hasNext() )
            return false;
          }

        return true;
        }

      for( Iterator iterator : iterators )
        {
        if( iterator.hasNext() )
          return true;
        }

      return false;
      }

    public Tuple next()
      {
      if( lastValues == null )
        return makeResult( initLastValues() );

      for( int i = iterators.length - 1; i >= 0; i-- )
        {
        if( iterators[ i ].hasNext() )
          {
          lastValues[ i ] = (Tuple) iterators[ i ].next();
          break;
          }

        // reset to first
        iterators[ i ] = getIterator( i );
        lastValues[ i ] = (Tuple) iterators[ i ].next();
        }

      return makeResult( lastValues );
      }

    private Tuple makeResult( Tuple[] lastValues )
      {
      Tuples.asModifiable( result );

      result = resultBuilder.makeResult( lastValues );

      if( LOG.isTraceEnabled() )
        LOG.trace( "tuple: {}", result.print() );

      return result;
      }

    public void remove()
      {
      // unsupported
      }
    }

  static interface TupleBuilder
    {
    Tuple makeResult( Tuple[] tuples );
    }
  }
