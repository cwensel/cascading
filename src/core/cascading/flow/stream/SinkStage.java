/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream;

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 *
 */
public class SinkStage extends ElementStage<TupleEntry, Void>
  {
  private final Tap sink;
  private TupleEntryCollector collector;

  public SinkStage( FlowProcess flowProcess, Tap sink )
    {
    super( flowProcess, sink );
    this.sink = sink;
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    // do not bind
    }

  @Override
  public void prepare()
    {
    try
      {
      collector = sink.openForWrite( flowProcess, getInput() );
      }
    catch( IOException exception )
      {
      throw new DuctException( "failed opening sink", exception );
      }
    }

  protected Object getInput()
    {
    return null;
    }

  @Override
  public void start( Duct previous )
    {
    // do nothing
    }

  @Override
  public void receive( Duct previous, TupleEntry tupleEntry )
    {
    try
      {
      collector.add( tupleEntry );
      }
    catch( OutOfMemoryError error )
      {
      handleReThrowableException( "out of memory, try increasing task memory allocation", error );
      }
    catch( CascadingException exception )
      {
      handleException( exception, tupleEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new DuctException( "internal error: " + tupleEntry.getTuple().print(), throwable ), tupleEntry );
      }
    }

  @Override
  public void complete( Duct previous )
    {
    // do nothing
    }

  @Override
  public void cleanup()
    {
    try
      {
      if( collector != null )
        collector.close();

      collector = null;
      }
    finally
      {
      super.cleanup();
      }
    }
  }
