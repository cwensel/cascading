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

package cascading.flow.stream;

import java.io.IOException;
import java.util.Iterator;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
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
      final TupleEntry tupleEntry = grouping.iterator.getTupleEntry();
      final Tuple valueNulledTuple = Tuples.setOnEmpty( tupleEntry, grouping.key );
      tupleEntry.setTuple( valueNulledTuple );

      incomingEntry = tupleEntry;

      operationCall.setOutputCollector( outputCollector );
      operationCall.setGroup( grouping.key );

      operationCall.setArgumentsIterator( new Iterator<TupleEntry>()
      {
      public boolean hasNext()
        {
        boolean hasNext = grouping.iterator.hasNext();

        if( !hasNext && !operationCall.isRetainValues() )
          tupleEntry.setTuple( valueNulledTuple ); // null out footer entries

        return hasNext;
        }

      public TupleEntry next()
        {
        argumentsEntry.setTuple( argumentsBuilder.makeResult( grouping.iterator.next().getTuple(), null ) );

        return argumentsEntry;
        }

      public void remove()
        {
        grouping.iterator.remove();
        }
      } );

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

  @Override
  public void complete( Duct previous )
    {
    next.complete( this );
    }
  }
