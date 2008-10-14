/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stack;

import java.util.Iterator;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.pipe.Every;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
class EveryBufferReducerStackElement extends ReducerStackElement
  {
  private final Every.EveryHandler everyHandler;

  public EveryBufferReducerStackElement( StackElement previous, FlowProcess flowProcess, Scope incomingScope, Tap trap, Every.EveryHandler everyHandler )
    {
    super( previous, flowProcess, incomingScope, trap );
    this.everyHandler = everyHandler;
    }

  public FlowElement getFlowElement()
    {
    return null;
    }

  @Override
  StackElement setNext( StackElement next )
    {
    try
      {
      return super.setNext( next );
      }
    finally
      {
      everyHandler.outputCollector = next;
      }
    }

  protected Fields resolveIncomingOperationFields()
    {
    return everyHandler.getEvery().resolveIncomingOperationFields( incomingScope );
    }

  @Override
  public void collect( Tuple key, Iterator values )
    {
    TupleEntry groupingEntry = getGroupingTupleEntry( key );

    try
      {
      everyHandler.operate( flowProcess, groupingEntry, null, (TupleEntryIterator) values );
      }
    catch( Exception exception )
      {
      handleException( exception, ( (Every.EveryBufferHandler) everyHandler ).getLastValue() );
      }
    }
  }