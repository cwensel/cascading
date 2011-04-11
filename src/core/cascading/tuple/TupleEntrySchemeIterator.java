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

package cascading.tuple;

import java.io.Closeable;
import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SourceCall;
import cascading.util.SingleCloseableInputIterator;

/**
 *
 */
public class TupleEntrySchemeIterator<Context, Input> extends TupleEntryIterator
  {
  private final FlowProcess flowProcess;
  private final Scheme<FlowProcess, Object, Input, Object, Context> scheme;
  private final CloseableIterator<Input> inputIterator;
  private SourceCall<Context, Input> sourceCall;

  private boolean isComplete = false;
  private boolean hasWaiting = false;
  private TupleException currentException;

  public TupleEntrySchemeIterator( FlowProcess flowProcess, Scheme<FlowProcess, Object, Input, Object, Context> scheme, Input input )
    {
    this( flowProcess, scheme, (CloseableIterator<Input>) new SingleCloseableInputIterator( (Closeable) input ) );
    }

  public TupleEntrySchemeIterator( FlowProcess flowProcess, Scheme<FlowProcess, Object, Input, Object, Context> scheme, CloseableIterator<Input> inputIterator )
    {
    super( scheme.getSourceFields() );
    this.flowProcess = flowProcess;
    this.scheme = scheme;
    this.inputIterator = inputIterator;

    if( !inputIterator.hasNext() )
      {
      isComplete = true;
      return;
      }

    this.sourceCall = new SourceCall<Context, Input>();

    this.sourceCall.setIncomingEntry( getTupleEntry() );
    this.sourceCall.setInput( inputIterator.next() );

    this.scheme.sourcePrepare( flowProcess, sourceCall );
    }

  @Override
  public boolean hasNext()
    {
    if( isComplete )
      return false;

    if( hasWaiting )
      return true;

    try
      {
      getNext();
      }
    catch( Exception exception )
      {
      currentException = new TupleException( "unable to read from input", exception );
      }

    if( !hasWaiting )
      isComplete = true;

    return !isComplete;
    }

  private TupleEntry getNext() throws IOException
    {
    Tuples.asModifiable( sourceCall.getIncomingEntry().getTuple() );
    hasWaiting = scheme.source( flowProcess, sourceCall );

    if( !hasWaiting && inputIterator.hasNext() )
      {
      sourceCall.setInput( inputIterator.next() );

      return getNext();
      }

    return getTupleEntry();
    }

  @Override
  public TupleEntry next()
    {
    if( currentException != null )
      throw currentException;

    if( isComplete )
      throw new IllegalStateException( "no next element" );

    try
      {
      if( hasWaiting )
        return getTupleEntry();

      return getNext();
      }
    catch( Exception exception )
      {
      throw new TupleException( "unable to read from input", exception );
      }
    finally
      {
      hasWaiting = false;
      }
    }

  @Override
  public void remove()
    {
    throw new UnsupportedOperationException( "may not remove elements from this iterator" );
    }

  @Override
  public void close() throws IOException
    {
    try
      {
      if( sourceCall != null )
        scheme.sourceCleanup( flowProcess, sourceCall );
      }
    finally
      {
      inputIterator.close();
      }
    }
  }
