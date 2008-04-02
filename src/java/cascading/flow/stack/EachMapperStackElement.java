/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.flow.stack;

import cascading.flow.FlowElement;
import cascading.flow.Scope;
import cascading.pipe.Each;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class EachMapperStackElement extends FlowMapperStackElement
  {
  private Each each;

  public EachMapperStackElement( FlowMapperStackElement previous, Scope incomingScope, Each each )
    {
    super( previous, incomingScope );
    this.each = each;
    }

  protected FlowElement getFlowElement()
    {
    return each;
    }

  @Override
  public void collect( Tuple tuple )
    {
    super.collect( tuple );

    operateEach( getTupleEntry( tuple ) );
    }

  private void operateEach( TupleEntry tupleEntry )
    {
    each.operate( next.getIncomingScope(), tupleEntry, next );
    }

  }
