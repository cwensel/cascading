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

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.Scope;
import cascading.tap.Tap;
import cascading.tap.TapCollector;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
class TapMapperStackElement extends MapperStackElement
  {
  private final Tap sink;
  private TapCollector tapCollector;

  public TapMapperStackElement( MapperStackElement previous, Scope incomingScope, Tap sink, boolean useTapCollector, JobConf conf ) throws IOException
    {
    super( previous, incomingScope );
    this.sink = sink;

    if( useTapCollector )
      this.tapCollector = sink.openForWrite( conf );
    }

  protected FlowElement getFlowElement()
    {
    return sink;
    }

  @Override
  public void collect( Tuple tuple )
    {
    super.collect( tuple );

    operateSink( getTupleEntry( tuple ) );
    }

  private void operateSink( TupleEntry tupleEntry )
    {
    try
      {
      if( tapCollector != null )
        sink.sink( tupleEntry.getFields(), tupleEntry.getTuple(), tapCollector );
      else
        sink.sink( tupleEntry.getFields(), tupleEntry.getTuple(), lastOutput );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error: " + tupleEntry.getTuple().print(), throwable );
      }
    }

  @Override
  public void close() throws IOException
    {
    if( tapCollector != null )
      tapCollector.close();
    }
  }
