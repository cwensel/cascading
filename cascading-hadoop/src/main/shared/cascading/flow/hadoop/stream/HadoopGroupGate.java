/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
public abstract class HadoopGroupGate extends GroupingSpliceGate
  {
  protected HadoopGroupByClosure closure;
  protected OutputCollector collector;

  private final boolean isBufferJoin;

  public HadoopGroupGate( FlowProcess flowProcess, Splice splice, IORole role )
    {
    super( flowProcess, splice, role );

    isBufferJoin = splice.getJoiner() instanceof BufferJoin;
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    if( role != IORole.sink )
      next = getNextFor( streamGraph );
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

  // todo: receive should receive the edge or ordinal so no lookup
  public void receive( Duct previous, int ordinal, TupleEntry incomingEntry )
    {
    // create a view over the incoming tuple
    Tuple groupTupleView = keyBuilder[ ordinal ].makeResult( incomingEntry.getTuple(), null );

    // reset keyTuple via groupTuple or groupSortTuple
    if( sortFields == null )
      groupTuple.reset( groupTupleView );
    else
      groupSortTuple.reset( groupTupleView, sortBuilder[ ordinal ].makeResult( incomingEntry.getTuple(), null ) );

    valueTuple.reset( valuesBuilder[ ordinal ].makeResult( incomingEntry.getTuple(), null ) );

    try
      {
      // keyTuple is a reference to either groupTuple or groupSortTuple
      wrapGroupingAndCollect( previous, ordinal, (Tuple) valueTuple, keyTuple );
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

  public void accept( Tuple key, Iterator<Tuple>[] values )
    {
    key = unwrapGrouping( key );

    closure.reset( key, values );

    // Buffer is using JoinerClosure directly
    if( !isBufferJoin )
      tupleEntryIterator.reset( splice.getJoiner().getIterator( closure ) );
    else
      tupleEntryIterator.reset( values );

    keyEntry.setTuple( closure.getGroupTuple( key ) );

    next.receive( this, 0, grouping );
    }

  protected abstract HadoopGroupByClosure createClosure();

  protected abstract void wrapGroupingAndCollect( Duct previous, int ordinal, Tuple valuesTuple, Tuple groupKey ) throws java.io.IOException;

  protected abstract Tuple unwrapGrouping( Tuple key );
  }
