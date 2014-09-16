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

package cascading.flow.tez.stream.element;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.hadoop.HadoopCoGroupClosure;
import cascading.flow.hadoop.util.TimedIterator;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.DuctException;
import cascading.flow.stream.graph.IORole;
import cascading.flow.tez.TezCoGroupClosure;
import cascading.pipe.CoGroup;
import cascading.tuple.Tuple;
import cascading.tuple.io.TuplePair;
import cascading.util.SortedListMultiMap;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TezCoGroupGate extends TezGroupGate
  {
  private static final Logger LOG = LoggerFactory.getLogger( TezCoGroupGate.class );

  protected TimedIterator<Tuple>[] timedIterators;

  public TezCoGroupGate( FlowProcess flowProcess, CoGroup coGroup, IORole role, LogicalOutput logicalOutput )
    {
    super( flowProcess, coGroup, role, logicalOutput );
    }

  public TezCoGroupGate( FlowProcess flowProcess, CoGroup coGroup, IORole role, SortedListMultiMap<Integer, LogicalInput> logicalInputs )
    {
    super( flowProcess, coGroup, role, logicalInputs );

    this.timedIterators = new TimedIterator[ logicalInputs.getKeys().size() ];

    for( int i = 0; i < this.timedIterators.length; i++ )
      this.timedIterators[ i ] = new TimedIterator<>( flowProcess, SliceCounters.Read_Duration, SliceCounters.Tuples_Read, i );
    }

  @Override
  protected Throwable reduce() throws Exception
    {
    Throwable localThrowable = null;

    try
      {
      start( this );

      SortedListMultiMap<Integer, KeyValuesReader> readers = getKeyValuesReaders();
      SortedListMultiMap<Tuple, Iterable<Tuple>> iterables = getSortedMultiMap( readers.getKeys().size() );

      Map.Entry<Tuple, List<Iterable<Tuple>>> current = forwardToNext( readers, iterables, null );
      List<Iterable<Tuple>> currentValues;

      while( current != null )
        {
        currentValues = current.getValue();

        for( int i = 0; i < timedIterators.length; i++ )
          timedIterators[ i ].reset( currentValues.get( i ) );

        accept( current.getKey(), timedIterators );

        current = forwardToNext( readers, iterables, currentValues );
        }

      complete( this );
      }
    catch( Throwable throwable )
      {
      if( !( throwable instanceof OutOfMemoryError ) )
        LOG.error( "caught throwable", throwable );

      return throwable;
      }

    return localThrowable;
    }

  private SortedListMultiMap<Integer, KeyValuesReader> getKeyValuesReaders() throws Exception
    {
    SortedListMultiMap<Integer, KeyValuesReader> readers = new SortedListMultiMap<>();

    for( Map.Entry<Integer, List<LogicalInput>> entry : logicalInputs.getEntries() )
      {
      for( LogicalInput logicalInput : entry.getValue() )
        readers.put( entry.getKey(), (KeyValuesReader) logicalInput.getReader() );
      }

    return readers;
    }

  private Map.Entry<Tuple, List<Iterable<Tuple>>> forwardToNext( SortedListMultiMap<Integer, KeyValuesReader> readers, SortedListMultiMap<Tuple, Iterable<Tuple>> iterables, List<Iterable<Tuple>> current )
    {
    try
      {
      int size = current == null ? readers.getKeys().size() : current.size();

      for( int ordinal = 0; ordinal < size; ordinal++ )
        {
        if( current != null && current.get( ordinal ) == null )
          continue;

        for( KeyValuesReader reader : readers.getValues( ordinal ) )
          {
          if( !reader.next() )
            continue;

          Tuple currentKey = (Tuple) reader.getCurrentKey();

          if( splice.isSorted() )
            currentKey = ( (TuplePair) currentKey ).getLhs();

          currentKey = getDelegatedTuple( currentKey ); // applies hasher

          Iterable<Tuple> currentValues = (Iterable) reader.getCurrentValues();

          iterables.set( currentKey, ordinal, currentValues );
          }
        }
      }
    catch( OutOfMemoryError error )
      {
      handleReThrowableException( "out of memory, try increasing task memory allocation", error );
      }
    catch( CascadingException exception )
      {
      handleException( exception, null );
      }
    catch( Throwable throwable )
      {
      handleException( new DuctException( "internal error", throwable ), null );
      }

    return iterables.pollFirstEntry();
    }

  private SortedListMultiMap<Tuple, Iterable<Tuple>> getSortedMultiMap( final int length )
    {
    return new SortedListMultiMap<Tuple, Iterable<Tuple>>( getKeyComparator(), length )
    {
    Iterable<Tuple>[] array = new Iterable[ length ];

    @Override
    protected List createCollection()
      {
      List<Iterable<Tuple>> collection = super.createCollection();

      Collections.addAll( collection, array ); // init with nulls

      return collection;
      }
    };
    }

  @Override
  protected HadoopCoGroupClosure createClosure()
    {
    return new TezCoGroupClosure( flowProcess, splice.getNumSelfJoins(), keyFields, valuesFields );
    }

  @Override
  protected void wrapGroupingAndCollect( Duct previous, Tuple valuesTuple, Tuple groupKey ) throws java.io.IOException
    {
    collector.collect( groupKey, valuesTuple );
    }

  @Override
  protected Tuple unwrapGrouping( Tuple key )
    {
    return key;
    }

  }
