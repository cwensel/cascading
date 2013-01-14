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

package cascading.flow.hadoop.stream;

import java.util.IdentityHashMap;
import java.util.Map;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.hadoop.HadoopCoGroupClosure;
import cascading.flow.stream.Duct;
import cascading.flow.stream.DuctException;
import cascading.flow.stream.SpliceGate;
import cascading.flow.stream.StreamGraph;
import cascading.pipe.CoGroup;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.io.IndexTuple;
import cascading.tuple.io.TuplePair;

/**
 *
 */
public class HadoopCoGroupGate extends HadoopGroupGate
  {
  private final Map<Duct, Integer> posMap = new IdentityHashMap<Duct, Integer>();

  public HadoopCoGroupGate( FlowProcess flowProcess, CoGroup coGroup, SpliceGate.Role role )
    {
    super( flowProcess, coGroup, role );
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    super.bind( streamGraph );

    if( role == Role.sink )
      orderDucts( streamGraph );
    }

  @Override
  public void prepare()
    {
    super.prepare();

    if( role != Role.sink )
      closure = new HadoopCoGroupClosure( flowProcess, splice.getNumSelfJoins(), keyFields, valuesFields );
    else
      makePosMap( posMap );
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    Integer pos = posMap.get( previous );

    Tuple groupTuple = keyBuilder[ pos ].makeResult( incomingEntry.getTuple(), null );
    Tuple sortTuple = sortFields == null ? null : sortBuilder[ pos ].makeResult( incomingEntry.getTuple(), null );
    Tuple valuesTuple = valuesBuilder[ pos ].makeResult( incomingEntry.getTuple(), null );

    Tuple groupKey = sortTuple == null ? groupTuple : new TuplePair( groupTuple, sortTuple );

    try
      {
      collector.collect( new IndexTuple( pos, groupKey ), new IndexTuple( pos, valuesTuple ) );
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
  protected Tuple unwrapGrouping( Tuple key )
    {
    return ( (IndexTuple) key ).getTuple();
    }
  }
