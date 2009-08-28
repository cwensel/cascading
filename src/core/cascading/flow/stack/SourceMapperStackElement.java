/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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
import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleIterator;

/**
 *
 */
class SourceMapperStackElement extends MapperStackElement
  {
  private final Tap source;
  private TupleEntryCollector outputCollector;

  private class SourcedCollector extends TupleEntryCollector
    {
    @Override
    protected void collect( Tuple tuple )
      {
      next.collect( tuple );
      }
    }

  public SourceMapperStackElement( FlowProcess flowProcess, Tap source ) throws IOException
    {
    super( null, flowProcess, null, null );
    this.source = source;
    this.outputCollector = new SourcedCollector();
    }

  protected FlowElement getFlowElement()
    {
    return source;
    }

  @Override
  public void start( TupleIterator tupleIterator )
    {
    operateSource( tupleIterator );
    }

  private void operateSource( TupleIterator tupleIterator )
    {
    try
      {
      while( tupleIterator.hasNext() )
        source.source( tupleIterator.next(), outputCollector );
      }
    catch( OutOfMemoryError error )
      {
      throw new StackException( "out of memory, try increasing task memory allocation", error );
      }
    catch( TapException exception )
      {
      throw new StackException( "exception writing to tap: " + source.toString(), exception );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error: " + source, throwable );
      }
    }

  public void prepare()
    {
    // do nothing
    }

  public void cleanup()
    {
    // do nothing
    }

  @Override
  public void close() throws IOException
    {
    try
      {
      super.close();
      }
    finally
      {
      if( outputCollector != null )
        outputCollector.close();
      }
    }
  }