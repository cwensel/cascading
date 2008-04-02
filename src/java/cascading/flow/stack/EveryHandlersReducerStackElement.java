/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
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
public class EveryHandlersReducerStackElement extends FlowReducerStackElement
  {
  private List<Every.EveryHandler> everyHandlers;

  public EveryHandlersReducerStackElement( FlowReducerStackElement previous, Scope incomingScope, List<Every.EveryHandler> everyHandlers )
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
    return next.resolveIncomingOperationFields();
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
