/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.planner.Scope;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.DuctException;
import cascading.flow.stream.element.InputSource;
import cascading.flow.stream.element.SpliceGate;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.Pipe;
import cascading.pipe.Splice;
import cascading.tap.hadoop.util.MeasuredOutputCollector;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.io.KeyTuple;
import cascading.tuple.io.ValueTuple;
import cascading.tuple.util.Resettable1;
import cascading.util.SortedListMultiMap;
import cascading.util.Util;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TezMergeGate extends SpliceGate<TupleEntry, TupleEntry> implements InputSource
  {
  private static final Logger LOG = LoggerFactory.getLogger( TezMergeGate.class );

  protected Collection<LogicalOutput> logicalOutputs;
  protected SortedListMultiMap<Integer, LogicalInput> logicalInputs;

  private MeasuredOutputCollector collector;
  private TupleEntry valueEntry;

  private final Resettable1<Tuple> keyTuple = new KeyTuple();

  public TezMergeGate( FlowProcess flowProcess, Splice splice, IORole role, Collection<LogicalOutput> logicalOutputs )
    {
    super( flowProcess, splice, role );

    if( logicalOutputs == null || logicalOutputs.isEmpty() )
      throw new IllegalArgumentException( "output must not be null or empty" );

    this.logicalOutputs = logicalOutputs;
    }

  public TezMergeGate( FlowProcess flowProcess, Splice splice, IORole role, SortedListMultiMap<Integer, LogicalInput> logicalInputs )
    {
    super( flowProcess, splice, role );

    if( logicalInputs == null || logicalInputs.getKeys().size() == 0 )
      throw new IllegalArgumentException( "inputs must not be null or empty" );

    Set<LogicalInput> inputs = new HashSet<>( logicalInputs.getValues() );

    if( inputs.size() != 1 )
      throw new IllegalArgumentException( "only supports a single input" );

    this.logicalInputs = logicalInputs;
    }

  @Override
  public void initialize()
    {
    super.initialize();

    Scope outgoingScope = Util.getFirst( outgoingScopes );
    valueEntry = new TupleEntry( outgoingScope.getOutValuesFields(), true );
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
    try
      {
      if( logicalInputs != null )
        {
        for( LogicalInput logicalInput : logicalInputs.getValues() )
          {
          LOG.info( "calling {}#start() on: {} {}, for {} inputs", logicalInput.getClass().getSimpleName(), getSplice(), Pipe.id( getSplice() ), logicalInputs.getValues().size() );

          logicalInput.start();
          }
        }

      if( logicalOutputs != null )
        {
        for( LogicalOutput logicalOutput : logicalOutputs )
          {
          LOG.info( "calling {}#start() on: {} {}", logicalOutput.getClass().getSimpleName(), getSplice(), Pipe.id( getSplice() ) );

          logicalOutput.start();
          }
        }
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to start input/output", exception );
      }

    if( role != IORole.source )
      collector = new MeasuredOutputCollector( flowProcess, SliceCounters.Write_Duration, createOutputCollector() );

    super.prepare();
    }

  @Override
  public void start( Duct previous )
    {
    if( next != null )
      super.start( previous );
    }

  @Override
  public void receive( Duct previous, int ordinal, TupleEntry incomingEntry )
    {
    try
      {
      keyTuple.reset( incomingEntry.getTuple() );

      collector.collect( keyTuple, ValueTuple.NULL );
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

  @Override
  public void run( Object input ) throws Throwable
    {
    Throwable throwable = map();

    if( throwable != null )
      throw throwable;
    }

  protected Throwable map() throws Exception
    {
    Throwable localThrowable = null;

    try
      {
      start( this );

      // if multiple ordinals, an input could be duplicated if sourcing multiple paths
      LogicalInput logicalInput = Util.getFirst( logicalInputs.getValues() );

      KeyValueReader reader = (KeyValueReader) logicalInput.getReader();

      while( reader.next() )
        {
        Tuple currentKey = (Tuple) reader.getCurrentKey();

        valueEntry.setTuple( currentKey );
        next.receive( this, 0, valueEntry );
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

  protected OutputCollector createOutputCollector()
    {
    if( logicalOutputs.size() == 1 )
      return new OldOutputCollector( Util.getFirst( logicalOutputs ) );

    final OutputCollector[] collectors = new OutputCollector[ logicalOutputs.size() ];

    int count = 0;
    for( LogicalOutput logicalOutput : logicalOutputs )
      collectors[ count++ ] = new OldOutputCollector( logicalOutput );

    return new OutputCollector()
      {
      @Override
      public void collect( Object key, Object value ) throws IOException
        {
        for( OutputCollector outputCollector : collectors )
          outputCollector.collect( key, value );
        }
      };
    }
  }
