/*
 * Copyright (c) 2007-2008 Chris K Wensel. All Rights Reserved.
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

import java.io.IOException;
import java.util.Iterator;

import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.Scope;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
abstract class MapperStackElement extends StackElement
  {
  /** Field incomingScope */
  final Scope incomingScope;
  /** Field incomingFields */
  private Fields incomingFields;
  /** Field tupleEntry */
  private TupleEntry tupleEntry;
  /** Field lastOutput */
  OutputCollector lastOutput;

  MapperStackElement( MapperStackElement previous, Scope incomingScope )
    {
    this.previous = previous;
    this.incomingScope = incomingScope;
    }

  public void setLastOutput( OutputCollector lastOutput )
    {
    this.lastOutput = lastOutput;
    }

  protected abstract FlowElement getFlowElement();

  Fields resolveIncomingFields()
    {
    if( incomingFields == null )
      incomingFields = getFlowElement().resolveFields( incomingScope );

    return incomingFields;
    }

  Scope getIncomingScope()
    {
    return incomingScope;
    }

  TupleEntry getTupleEntry( Tuple tuple )
    {
    if( tupleEntry == null )
      tupleEntry = new TupleEntry( resolveIncomingFields() );

    tupleEntry.setTuple( tuple );

    return tupleEntry;
    }

  public void collect( Tuple tuple )
    {
    if( tuple.isEmpty() )
      throw new FlowException( "may not collect an empty tuple" );
    }

  public void collect( Tuple key, Iterator tupleIterator )
    {
    throw new UnsupportedOperationException( "collect should never be called" );
    }

  @Override
  public String toString()
    {
    return getFlowElement().toString();
    }

  public void close() throws IOException
    {
    }
  }
