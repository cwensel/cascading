/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
public class TupleEntrySchemeIterator<SourceContext, SinkContext, Input> extends TupleEntryIterator
  {
  private final FlowProcess flowProcess;
  private final Scheme<FlowProcess, Object, Input, Object, SourceContext, SinkContext> scheme;
  private final CloseableIterator<Input> inputIterator;
  private SourceCall<SourceContext, Input> sourceCall;

  private boolean isComplete = false;
  private boolean hasWaiting = false;
  private TupleException currentException;

  public TupleEntrySchemeIterator( FlowProcess flowProcess, Scheme<FlowProcess, Object, Input, Object, SourceContext, SinkContext> scheme, Input input )
    {
    this( flowProcess, scheme, (CloseableIterator<Input>) new SingleCloseableInputIterator( (Closeable) input ) );
    }

  public TupleEntrySchemeIterator( FlowProcess flowProcess, Scheme<FlowProcess, Object, Input, Object, SourceContext, SinkContext> scheme, CloseableIterator<Input> inputIterator )
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

    this.sourceCall = new SourceCall<SourceContext, Input>();

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
