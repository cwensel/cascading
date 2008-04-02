/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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
import java.util.List;

import cascading.flow.FlowElement;
import cascading.flow.Scope;
import cascading.pipe.Every;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
class EveryHandlersReducerStackElement extends ReducerStackElement
  {
  private final List<Every.EveryHandler> everyHandlers;

  public EveryHandlersReducerStackElement( StackElement previous, Scope incomingScope, List<Every.EveryHandler> everyHandlers )
    {
    super( previous, incomingScope );
    this.everyHandlers = everyHandlers;
    }

  public FlowElement getFlowElement()
    {
    return null;
    }

  protected Fields resolveIncomingOperationFields()
    {
    return ( (ReducerStackElement) next ).resolveIncomingOperationFields();
    }

  public void collect( Tuple key, Iterator values )
    {
    operateEveryHandlers( getGroupingTupleEntry( key ), values );
    }

  private void operateEveryHandlers( TupleEntry keyEntry, Iterator values )
    {
    for( Every.EveryHandler everyHandler : everyHandlers )
      everyHandler.start( keyEntry );

    while( values.hasNext() )
      {
      TupleEntry valueEntry = (TupleEntry) values.next();

      for( Every.EveryHandler handler : everyHandlers )
        handler.operate( valueEntry );
      }

    next.collect( keyEntry.getTuple() );
    }
  }
