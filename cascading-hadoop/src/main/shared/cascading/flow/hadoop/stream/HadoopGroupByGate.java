/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.hadoop.HadoopGroupByClosure;
import cascading.flow.stream.Duct;
import cascading.flow.stream.DuctException;
import cascading.pipe.GroupBy;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.io.TuplePair;

/**
 *
 */
public class HadoopGroupByGate extends HadoopGroupGate
  {
  public HadoopGroupByGate( FlowProcess flowProcess, GroupBy groupBy, Role role )
    {
    super( flowProcess, groupBy, role );
    }

  @Override
  public void prepare()
    {
    super.prepare();

    if( role != Role.sink )
      closure = new HadoopGroupByClosure( flowProcess, keyFields, valuesFields );

    if( grouping != null && splice.getJoinDeclaredFields() != null && splice.getJoinDeclaredFields().isNone() )
      grouping.joinerClosure = closure;
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    // always use pos == 0 since all key/value/sort fields are guaranteed to be the same
    Tuple groupTuple = keyBuilder[ 0 ].makeResult( incomingEntry.getTuple(), null );
    Tuple sortTuple = sortFields == null ? null : sortBuilder[ 0 ].makeResult( incomingEntry.getTuple(), null );
    Tuple valuesTuple = valuesBuilder[ 0 ].makeResult( incomingEntry.getTuple(), null ); // nulls out the dupe values

    Tuple groupKey = sortTuple == null ? groupTuple : new TuplePair( groupTuple, sortTuple );

    try
      {
      collector.collect( groupKey, valuesTuple );
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
    return sortFields == null ? key : ( (TuplePair) key ).getLhs();
    }
  }
