/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.flow.stack;

import cascading.flow.FlowElement;
import cascading.flow.Scope;
import cascading.pipe.Every;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class EveryHandlerReducerStackElement extends FlowReducerStackElement
  {
  private Every.EveryHandler everyHandler;

  public EveryHandlerReducerStackElement( FlowReducerStackElement previous, Scope incomingScope, Every.EveryHandler everyHandler )
    {
    super( previous, incomingScope );
    this.everyHandler = everyHandler;
    }

  public FlowElement getFlowElement()
    {
    return null;
    }

  protected Fields resolveIncomingOperationFields()
    {
    return everyHandler.getEvery().resolveIncomingOperationFields( incomingScope );
    }

  public void collect( Tuple tuple )
    {
    operateEveryHandler( getGroupingTupleEntry( tuple ) );
    }

  private void operateEveryHandler( TupleEntry keyEntry )
    {
    everyHandler.complete( keyEntry, next );
    }
  }
