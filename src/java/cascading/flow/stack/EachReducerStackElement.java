/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.flow.stack;

import java.util.Iterator;

import cascading.flow.FlowElement;
import cascading.flow.Scope;
import cascading.pipe.Each;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class EachReducerStackElement extends FlowReducerStackElement
  {
  private Each each;

  public EachReducerStackElement( FlowReducerStackElement previous, Scope incomingScope, Each each )
    {
    super( previous, incomingScope );
    this.each = each;
    }

  public FlowElement getFlowElement()
    {
    return each;
    }

  public void collect( Tuple key, Iterator values )
    {
    operateEach( key, values );
    }

  public void collect( Tuple tuple )
    {
    operateEach( getTupleEntry( tuple ) );
    }

  private void operateEach( Tuple key, Iterator values )
    {
    while( values.hasNext() )
      operateEach( (TupleEntry) values.next() );
    }

  private void operateEach( TupleEntry tupleEntry )
    {
    each.operate( next.getIncomingScope(), tupleEntry, next );
    }
  }
