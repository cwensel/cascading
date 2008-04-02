/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.flow.stack;

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.Scope;
import cascading.pipe.Group;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class GroupMapperStackElement extends FlowMapperStackElement
  {
  private Group group;
  private Scope outgoingScope;

  public GroupMapperStackElement( FlowMapperStackElement previous, Scope incomingScope, Group group, Scope outgoingScope )
    {
    super( previous, incomingScope );
    this.group = group;
    this.outgoingScope = outgoingScope;
    }

  protected FlowElement getFlowElement()
    {
    return group;
    }

  @Override
  public void collect( Tuple tuple )
    {
    super.collect( tuple );

    operateGroup( getTupleEntry( tuple ) );
    }

  private void operateGroup( TupleEntry tupleEntry )
    {
    try
      {
      group.makeReduceGrouping( incomingScope, outgoingScope, tupleEntry, lastOutput );
      }
    catch( IOException exception )
      {
      throw new FlowException( "failed writing output", exception );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error", throwable );
      }
    }
  }
