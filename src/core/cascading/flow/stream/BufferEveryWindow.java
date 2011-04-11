/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
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

    outputCollector = new TupleEntryCollector( outgoingScopes.get( 0 ).getDeclaredFields() )
    {
    @Override
    protected void collect( TupleEntry resultEntry ) throws IOException
      {
      Tuple outgoing = every.makeResult( outgoingSelector, incomingEntry, remainderFields, resultEntry, resultEntry.getTuple() );

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
      final Tuple valueNulledTuple = Tuples.setOnEmpty( tupleEntry, grouping.group );
      tupleEntry.setTuple( valueNulledTuple );

      incomingEntry = tupleEntry;

      operationCall.setOutputCollector( outputCollector );
      operationCall.setGroup( grouping.group );

      operationCall.setArgumentsIterator( new Iterator<TupleEntry>()
      {
      public boolean hasNext()
        {
        boolean hasNext = grouping.iterator.hasNext();

        if( !hasNext )
          tupleEntry.setTuple( valueNulledTuple ); // null out footer entries

        return hasNext;
        }

      public TupleEntry next()
        {
        argumentsEntry.setTuple( grouping.iterator.next().selectTuple( argumentsSelector ) );

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
      handleException( exception, incomingEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new OperatorException( every, "operator Every failed executing operation: " + every.getOperation(), throwable ), incomingEntry );
      }
    }

  @Override
  public void complete( Duct previous )
    {
    next.complete( this );
    }
  }
