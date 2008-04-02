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

import cascading.flow.FlowElement;
import cascading.flow.Scope;
import cascading.pipe.Each;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
class EachReducerStackElement extends ReducerStackElement
  {
  private final Each each;

  public EachReducerStackElement( StackElement previous, Scope incomingScope, Each each )
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
    each.operate( ( (ReducerStackElement) next ).getIncomingScope(), tupleEntry, next );
    }
  }
