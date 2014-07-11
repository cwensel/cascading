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

package cascading.flow.hadoop.stream;

import java.util.Iterator;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.hadoop.HadoopGroupByClosure;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.DuctException;
import cascading.flow.stream.element.GroupingSpliceGate;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.Splice;
import cascading.pipe.joiner.BufferJoin;
import cascading.tap.hadoop.util.MeasuredOutputCollector;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.io.TuplePair;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
public abstract class HadoopGroupGate extends GroupingSpliceGate
  {
  protected HadoopGroupByClosure closure;
  protected OutputCollector collector;

  public HadoopGroupGate( FlowProcess flowProcess, Splice splice, IORole role )
    {
    super( flowProcess, splice, role );
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    if( role != IORole.sink )
      next = getNextFor( streamGraph );

    if( role == IORole.sink )
      setOrdinalMap( streamGraph );
    }

  @Override
  public void prepare()
    {
    if( role != IORole.source )
      collector = new MeasuredOutputCollector( flowProcess, SliceCounters.Write_Duration, createOutputCollector() );

    if( role != IORole.sink )
      closure = createClosure();

    if( grouping != null && splice.getJoinDeclaredFields() != null && splice.getJoinDeclaredFields().isNone() )
      grouping.joinerClosure = closure;
    }

  protected abstract OutputCollector createOutputCollector();

  @Override
  public void start( Duct previous )
    {
    if( next != null )
      super.start( previous );
    }

  public void receive( Duct previous, TupleEntry incomingEntry ) // todo: receive should receive the edge or ordinal so no lookup
  {
  Integer pos = ordinalMap.get( previous ); // todo: when posMap size == 1, pos is always zero -- optimize #get() out

  Tuple groupTuple = keyBuilder[ pos ].makeResult( incomingEntry.getTuple(), null );
  Tuple sortTuple = sortFields == null ? null : sortBuilder[ pos ].makeResult( incomingEntry.getTuple(), null );
  Tuple valuesTuple = valuesBuilder[ pos ].makeResult( incomingEntry.getTuple(), null );

  Tuple groupKey = sortTuple == null ? groupTuple : new TuplePair( groupTuple, sortTuple );

  try
    {
    wrapGroupingAndCollect( previous, valuesTuple, groupKey );
    flowProcess.increment( SliceCounters.Tuples_Written, 1 );
    }
  catch( OutOfMemoryError error )
    {
    handleReThrowableException( "out of memory, try increasing task memory allocation", error );
    }
  catch( CascadingException exception )
    {
    handleException( exception, incomingEntry );
    }
  catch( Throwable throwable )
    {
    handleException( new DuctException( "internal error: " + incomingEntry.getTuple().print(), throwable ), incomingEntry );
    }
  }

  @Override
  public void complete( Duct previous )
    {
    if( next != null )
      super.complete( previous );
    }

  public void accept( Tuple key, Iterator<Tuple>... values )
    {
    key = unwrapGrouping( key );

    closure.reset( key, values );

    // Buffer is using JoinerClosure directly
    if( !( splice.getJoiner() instanceof BufferJoin ) )
      tupleEntryIterator.reset( splice.getJoiner().getIterator( closure ) );
    else
      tupleEntryIterator.reset( values );

    keyEntry.setTuple( closure.getGroupTuple( key ) );

    next.receive( this, grouping );
    }

  protected abstract HadoopGroupByClosure createClosure();

  protected abstract void wrapGroupingAndCollect( Duct previous, Tuple valuesTuple, Tuple groupKey ) throws java.io.IOException;

  protected abstract Tuple unwrapGrouping( Tuple key );
  }
