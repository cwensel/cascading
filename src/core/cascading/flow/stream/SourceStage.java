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

import java.util.concurrent.Callable;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SourceStage extends ElementStage<Void, TupleEntry> implements Callable<Throwable>
  {
  private static final Logger LOG = LoggerFactory.getLogger( SourceStage.class );

  private final Tap source;

  public SourceStage( FlowProcess flowProcess, Tap source )
    {
    super( flowProcess, source );
    this.source = source;
    }

  @Override
  public Throwable call() throws Exception
    {
    return map( null );
    }

  public void run( Object input ) throws Throwable
    {
    Throwable throwable = map( input );

    if( throwable != null )
      throw throwable;
    }

  private Throwable map( Object input )
    {
    Throwable localThrowable = null;
    TupleEntryIterator iterator = null;

    try
      {
      next.start( this );

      iterator = source.openForRead( flowProcess, input );

      while( iterator.hasNext() )
        {
        TupleEntry tupleEntry = null;

        try
          {
          tupleEntry = iterator.next();
          }
        catch( OutOfMemoryError error )
          {
          handleReThrowableException( "out of memory, try increasing task memory allocation", error );
          }
        catch( CascadingException exception )
          {
          handleException( exception, null );
          }
        catch( Throwable throwable )
          {
          handleException( new DuctException( "internal error", throwable ), null );
          }

        next.receive( this, tupleEntry );
        }

      next.complete( this );
      }
    catch( Throwable exception )
      {
      LOG.error( "caught throwable", exception );
      localThrowable = exception;
      }
    finally
      {
      try
        {
        if( iterator != null )
          iterator.close();
        }
      catch( Throwable exception )
        {
        LOG.warn( "failed closing iterator", exception );

        if( localThrowable != null )
          localThrowable = exception;
        }
      }

    return localThrowable;
    }

  @Override
  public void initialize()
    {
    }

  @Override
  public void receive( Duct previous, Void nada )
    {
    throw new UnsupportedOperationException( "use call() instead" );
    }

  }
