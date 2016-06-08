/*
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

package cascading.flow.stream.element;

import java.io.IOException;
import java.util.Iterator;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Grouping;
import cascading.flow.stream.duct.OpenWindow;
import cascading.operation.Buffer;
import cascading.pipe.Every;
import cascading.pipe.OperatorException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.Tuples;

/**
 *
 */
public class BufferEveryWindow extends EveryStage<Grouping<TupleEntry, TupleEntryIterator>> implements OpenWindow
  {
  Buffer buffer;

  public BufferEveryWindow( FlowProcess flowProcess, Every every )
    {
    super( flowProcess, every );
    }

  @Override
  public void initialize()
    {
    super.initialize();

    buffer = every.getBuffer();

    outputCollector = new TupleEntryCollector( getOperationDeclaredFields() )
    {
    @Override
    protected void collect( TupleEntry resultEntry ) throws IOException
      {
      Tuple outgoing = outgoingBuilder.makeResult( incomingEntry.getTuple(), resultEntry.getTuple() );

      outgoingEntry.setTuple( outgoing );

      try
        {
        next.receive( BufferEveryWindow.this, outgoingEntry );
        }
      finally
        {
        Tuples.asModifiable( outgoing );
        }
      }
    };
    }

  @Override
  protected Fields getIncomingPassThroughFields()
    {
    return incomingScopes.get( 0 ).getIncomingBufferPassThroughFields();
    }

  @Override
  protected Fields getIncomingArgumentsFields()
    {
    return incomingScopes.get( 0 ).getIncomingBufferArgumentFields();
    }

  @Override
  protected Fields getOutgoingSelector()
    {
    return outgoingScopes.get( 0 ).getOutGroupingSelector();
    }

  @Override
  public void start( Duct previous )
    {
    next.start( this );
    }

  @Override
  public void receive( Duct previous, final Grouping<TupleEntry, TupleEntryIterator> grouping )
    {
    try
      {
      // we want to null out any 'values' before and after the iterator begins/ends
      // this allows buffers to emit tuples before next() and when hasNext() return false;
      final TupleEntry tupleEntry = grouping.joinIterator.getTupleEntry();
      incomingEntry = tupleEntry;

      // if Fields.NONE are declared on the CoGroup, we don't provide arguments, only the joinerClosure
      if( !tupleEntry.getFields().isNone() )
        {
        final Tuple valueNulledTuple = Tuples.setOnEmpty( tupleEntry, grouping.key );
        tupleEntry.setTuple( valueNulledTuple );

        operationCall.setArgumentsIterator( createArgumentsIterator( grouping, tupleEntry, valueNulledTuple ) );
        }

      operationCall.setOutputCollector( outputCollector );
      operationCall.setJoinerClosure( grouping.joinerClosure );
      operationCall.setGroup( grouping.key );

      buffer.operate( flowProcess, operationCall );
      }
    catch( CascadingException exception )
      {
      handleException( exception, argumentsEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new OperatorException( every, "operator Every failed executing operation: " + every.getOperation(), throwable ), argumentsEntry );
      }
    }

  private Iterator<TupleEntry> createArgumentsIterator( final Grouping<TupleEntry, TupleEntryIterator> grouping, final TupleEntry tupleEntry, final Tuple valueNulledTuple )
    {
    return new Iterator<TupleEntry>()
    {
    public boolean hasNext()
      {
      boolean hasNext = grouping.joinIterator.hasNext();

      if( !hasNext && !operationCall.isRetainValues() )
        tupleEntry.setTuple( valueNulledTuple ); // null out footer entries

      return hasNext;
      }

    public TupleEntry next()
      {
      argumentsEntry.setTuple( argumentsBuilder.makeResult( grouping.joinIterator.next().getTuple(), null ) );

      return argumentsEntry;
      }

    public void remove()
      {
      grouping.joinIterator.remove();
      }
    };
    }
  }
