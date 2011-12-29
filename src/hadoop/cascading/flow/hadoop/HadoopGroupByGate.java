/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.stream.Duct;
import cascading.flow.stream.DuctException;
import cascading.pipe.Group;
import cascading.pipe.cogroup.GroupByClosure;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TuplePair;
import cascading.tuple.Tuples;

/**
 *
 */
public class HadoopGroupByGate extends HadoopGroupGate
  {
  public HadoopGroupByGate( FlowProcess flowProcess, Group group, Role role )
    {
    super( flowProcess, group, role );
    }

  @Override
  public void prepare()
    {
    super.prepare();

    if( role != Role.sink )
      closure = new GroupByClosure( flowProcess, groupFields, valuesFields );
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    Tuple groupTuple = incomingEntry.selectTuple( groupFields[ 0 ] );
    Tuple sortTuple = sortFields == null ? null : incomingEntry.selectTuple( sortFields[ 0 ] );
    Tuple valuesTuple = Tuples.nulledCopy( incomingEntry, groupFields[ 0 ] );

    Tuple groupKey = sortTuple == null ? groupTuple : new TuplePair( groupTuple, sortTuple );

    try
      {
      collector.collect( groupKey, valuesTuple );
      flowProcess.increment( MapReduceCounters.Map_Tuples_Written, 1 );
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
    return sortFields == null ? (Tuple) key : ( (TuplePair) key ).getLhs();
    }
  }
