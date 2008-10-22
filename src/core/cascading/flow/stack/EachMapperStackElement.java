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

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.pipe.Each;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
class EachMapperStackElement extends MapperStackElement
  {
  private final Each each;
  private Each.EachHandler eachHandler;

  public EachMapperStackElement( MapperStackElement previous, FlowProcess flowProcess, Scope incomingScope, Tap trap, Each each )
    {
    super( previous, flowProcess, incomingScope, trap );
    this.each = each;
    }

  protected FlowElement getFlowElement()
    {
    return each;
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
      eachHandler = each.getHandler( next, ( (MapperStackElement) next ).getIncomingScope() );
      }
    }

  public void prepare()
    {
    eachHandler.prepare( flowProcess );
    }

  public void cleanup()
    {
    eachHandler.cleanup( flowProcess );
    }

  @Override
  public void collect( Tuple tuple )
    {
    super.collect( tuple );

    operateEach( getTupleEntry( tuple ) );
    }

  private void operateEach( TupleEntry tupleEntry )
    {
    try
      {
      eachHandler.operate( flowProcess, tupleEntry );
      }
    catch( Exception exception )
      {
      handleException( exception, tupleEntry );
      }
    }
  }
