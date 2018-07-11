/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.hadoop.HadoopGroupByClosure;
import cascading.flow.hadoop.util.TimedIterator;
import cascading.flow.stream.StopDataNotificationException;
import cascading.flow.stream.graph.IORole;
import cascading.flow.tez.TezGroupByClosure;
import cascading.flow.tez.util.SecondarySortKeyValuesReader;
import cascading.pipe.GroupBy;
import cascading.tuple.Tuple;
import cascading.tuple.io.TuplePair;
import cascading.util.LogUtil;
import cascading.util.SortedListMultiMap;
import cascading.util.Util;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TezGroupByGate extends TezGroupGate
  {
  private static final Logger LOG = LoggerFactory.getLogger( TezGroupByGate.class );

  protected TimedIterator[] timedIterators;

  public TezGroupByGate( FlowProcess flowProcess, GroupBy groupBy, IORole role, LogicalOutput logicalOutput )
    {
    super( flowProcess, groupBy, role, logicalOutput );
    }

  public TezGroupByGate( FlowProcess flowProcess, GroupBy groupBy, IORole role, SortedListMultiMap<Integer, LogicalInput> logicalInputs )
    {
    super( flowProcess, groupBy, role, logicalInputs );

    this.timedIterators = TimedIterator.iterators( new TimedIterator<>( flowProcess, SliceCounters.Read_Duration, SliceCounters.Tuples_Read ) );
    }

  protected Throwable reduce() throws Exception
    {
    try
      {
      start( this );

      // if multiple ordinals, an input could be duplicated if sourcing multiple paths
      LogicalInput logicalInput = Util.getFirst( logicalInputs.getValues() );

      KeyValuesReader reader = (KeyValuesReader) logicalInput.getReader();

      if( sortFields != null )
        reader = new SecondarySortKeyValuesReader( reader, groupComparators[ 0 ] );

      while( reader.next() )
        {
        Tuple currentKey = (Tuple) reader.getCurrentKey(); // if secondary sorting, is a TuplePair
        Iterable currentValues = reader.getCurrentValues();

        timedIterators[ 0 ].reset( currentValues );

        try
          {
          accept( currentKey, timedIterators ); // will unwrap the TuplePair
          }
        catch( StopDataNotificationException exception )
          {
          LogUtil.logWarnOnce( LOG, "received unsupported stop data notification, ignoring: {}", exception.getMessage() );
          }
        }

      complete( this );
      }
    catch( Throwable throwable )
      {
      if( !( throwable instanceof OutOfMemoryError ) )
        LOG.error( "caught throwable", throwable );

      return throwable;
      }

    return null;
    }

  @Override
  protected HadoopGroupByClosure createClosure()
    {
    return new TezGroupByClosure( flowProcess, keyFields, valuesFields );
    }

  @Override
  protected Tuple unwrapGrouping( Tuple key )
    {
    // copying the lhs key during secondary sorting prevents the key from advancing at the end of the
    // aggregation iterator
    return sortFields == null ? key : new Tuple( ( (TuplePair) key ).getLhs() );
    }
  }
