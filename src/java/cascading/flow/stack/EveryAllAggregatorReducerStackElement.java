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
import java.util.List;
import java.util.Map;

import cascading.flow.FlowElement;
import cascading.flow.FlowSession;
import cascading.flow.Scope;
import cascading.pipe.Every;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
class EveryAllAggregatorReducerStackElement extends ReducerStackElement
  {
  private final Map<String, Tap> traps;
  private final List<Every.EveryHandler> everyHandlers;

  public EveryAllAggregatorReducerStackElement( StackElement previous, FlowSession flowSession, Scope incomingScope, Map<String, Tap> traps, List<Every.EveryHandler> everyHandlers )
    {
    super( previous, flowSession, incomingScope, null );
    this.traps = traps;
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
    for( Every.EveryHandler handler : everyHandlers )
      {
      try
        {
        handler.start( flowSession, keyEntry );
        }
      catch( Exception exception )
        {
        handleException( traps.get( handler.getEvery().getName() ), exception, keyEntry );
        }
      }

    while( values.hasNext() )
      {
      TupleEntry valueEntry = (TupleEntry) values.next();

      for( Every.EveryHandler handler : everyHandlers )
        {
        try
          {
          handler.operate( flowSession, null, valueEntry, null );
          }
        catch( Exception exception )
          {
          handleException( traps.get( handler.getEvery().getName() ), exception, valueEntry );
          }
        }
      }

    next.collect( keyEntry.getTuple() );
    }
  }
