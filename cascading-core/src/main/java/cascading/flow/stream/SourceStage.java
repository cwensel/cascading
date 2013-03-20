/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.flow.stream;

import java.util.concurrent.Callable;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.StepCounters;
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

      // input may be null
      iterator = source.openForRead( flowProcess, input );

      while( iterator.hasNext() )
        {
        TupleEntry tupleEntry;

        try
          {
          tupleEntry = iterator.next();
          flowProcess.increment( StepCounters.Tuples_Read, 1 );
          flowProcess.increment( SliceCounters.Tuples_Read, 1 );
          }
        catch( OutOfMemoryError error )
          {
          handleReThrowableException( "out of memory, try increasing task memory allocation", error );
          continue;
          }
        catch( CascadingException exception )
          {
          handleException( exception, null );
          continue;
          }
        catch( Throwable throwable )
          {
          handleException( new DuctException( "internal error", throwable ), null );
          continue;
          }

        next.receive( this, tupleEntry );
        }

      next.complete( this );
      }
    catch( Throwable throwable )
      {
      if( !( throwable instanceof OutOfMemoryError ) )
        LOG.error( "caught throwable", throwable );

      return throwable;
      }
    finally
      {
      try
        {
        if( iterator != null )
          iterator.close();
        }
      catch( Throwable currentThrowable )
        {
        if( !( currentThrowable instanceof OutOfMemoryError ) )
          LOG.warn( "failed closing iterator", currentThrowable );

        localThrowable = currentThrowable;
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
