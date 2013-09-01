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

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.HadoopGroupByClosure;
import cascading.flow.stream.Duct;
import cascading.flow.stream.SpliceGate;
import cascading.flow.stream.StreamGraph;
import cascading.pipe.Splice;
import cascading.pipe.joiner.BufferJoin;
import cascading.tap.hadoop.util.MeasuredOutputCollector;
import cascading.tuple.Tuple;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
public abstract class HadoopGroupGate extends SpliceGate
  {
  protected HadoopGroupByClosure closure;
  protected OutputCollector collector;

  public HadoopGroupGate( FlowProcess flowProcess, Splice splice, Role role )
    {
    super( flowProcess, splice, role );
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    allPrevious = getAllPreviousFor( streamGraph );

    if( role != Role.sink )
      next = getNextFor( streamGraph );
    }

  @Override
  public void prepare()
    {
    collector = new MeasuredOutputCollector( flowProcess, SliceCounters.Write_Duration, ( (HadoopFlowProcess) flowProcess ).getOutputCollector() );
    }

  @Override
  public void start( Duct previous )
    {
    if( next != null )
      super.start( previous );
    }

  @Override
  public void complete( Duct previous )
    {
    if( next != null )
      super.complete( previous );
    }

  public void run( Tuple key, Iterator values )
    {
    key = unwrapGrouping( key );

    closure.reset( key, values );

    // Buffer is using JoinerClosure directly
    if( !( splice.getJoiner() instanceof BufferJoin ) )
      values = splice.getJoiner().getIterator( closure );

    keyEntry.setTuple( closure.getGroupTuple( key ) );
    tupleEntryIterator.reset( values );

    next.receive( this, grouping );
    }

  protected abstract Tuple unwrapGrouping( Tuple key );
  }
