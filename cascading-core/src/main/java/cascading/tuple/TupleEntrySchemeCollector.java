/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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
import java.io.Flushable;
import java.io.IOException;
import java.util.function.Supplier;

import cascading.flow.FlowProcess;
import cascading.scheme.ConcreteCall;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.TapException;

/**
 * Class TupleEntrySchemeCollector is a helper class for wrapping a {@link Scheme} instance, calling
 * {@link Scheme#sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)} on every call to {@link #add(TupleEntry)}
 * or {@link #add(Tuple)}.
 * <p>
 * Use this class inside a custom {@link cascading.tap.Tap} when overriding the
 * {@link cascading.tap.Tap#openForWrite(cascading.flow.FlowProcess)} method.
 */
public class TupleEntrySchemeCollector<Config, Output> extends TupleEntryCollector
  {
  private final FlowProcess<? extends Config> flowProcess;
  private final Scheme<Config, ?, Output, ?, Object> scheme;

  protected final ConcreteCall<Object, Output> sinkCall;
  private Supplier<String> loggableIdentifier = () -> "'unknown'";
  private boolean prepared = false;

  @Deprecated
  public TupleEntrySchemeCollector( FlowProcess<? extends Config> flowProcess, Scheme scheme )
    {
    this( flowProcess, scheme, null, null );
    }

  @Deprecated
  public TupleEntrySchemeCollector( FlowProcess<? extends Config> flowProcess, Scheme scheme, String loggableIdentifier )
    {
    this( flowProcess, scheme, null, loggableIdentifier );
    }

  @Deprecated
  public TupleEntrySchemeCollector( FlowProcess<? extends Config> flowProcess, Scheme scheme, Output output )
    {
    this( flowProcess, scheme, output, null );
    }

  public TupleEntrySchemeCollector( FlowProcess<? extends Config> flowProcess, Tap tap, Output output )
    {
    this( flowProcess, tap, tap.getScheme(), output, tap.getIdentifier() );
    }

  @Deprecated
  public TupleEntrySchemeCollector( FlowProcess<? extends Config> flowProcess, Scheme scheme, Output output, String loggableIdentifier )
    {
    this( flowProcess, null, scheme, output, loggableIdentifier );
    }

  public TupleEntrySchemeCollector( FlowProcess<? extends Config> flowProcess, Tap tap, Scheme scheme )
    {
    this( flowProcess, tap, scheme, null, (Supplier<String>) null );
    }

  public TupleEntrySchemeCollector( FlowProcess<? extends Config> flowProcess, Tap tap, Scheme scheme, String loggableIdentifier )
    {
    this( flowProcess, tap, scheme, null, loggableIdentifier );
    }

  public TupleEntrySchemeCollector( FlowProcess<? extends Config> flowProcess, Tap tap, Scheme scheme, Output output )
    {
    this( flowProcess, tap, scheme, output, (Supplier<String>) null );
    }

  public TupleEntrySchemeCollector( FlowProcess<? extends Config> flowProcess, Tap tap, Scheme scheme, Output output, String loggableIdentifier )
    {
    this( flowProcess, tap, scheme, output, loggableIdentifier == null ? null : () -> loggableIdentifier );
    }

  public TupleEntrySchemeCollector( FlowProcess<? extends Config> flowProcess, Tap tap, Scheme scheme, Output output, Supplier<String> loggableIdentifier )
    {
    super( Fields.asDeclaration( scheme.getSinkFields() ) );
    this.flowProcess = flowProcess;
    this.scheme = scheme;

    if( loggableIdentifier != null )
      this.loggableIdentifier = loggableIdentifier; // only used for logging

    this.sinkCall = createSinkCall();
    this.sinkCall.setTap( tap );
    this.sinkCall.setOutgoingEntry( this.tupleEntry ); // created in super ctor

    if( output != null )
      setOutput( output );
    }

  /**
   * Override to provide custom ConcreteCall implementation to expose Tap level resources to the underlying Scheme.
   *
   * @return a new ConcreteCall instance
   */
  protected <Context, IO> ConcreteCall<Context, IO> createSinkCall()
    {
    return new ConcreteCall<>();
    }

  protected FlowProcess<? extends Config> getFlowProcess()
    {
    return flowProcess;
    }

  @Override
  public void setFields( Fields declared )
    {
    super.setFields( declared );

    if( this.sinkCall != null )
      this.sinkCall.setOutgoingEntry( this.tupleEntry );
    }

  protected Output getOutput()
    {
    return sinkCall.getOutput();
    }

  protected void setOutput( Output output )
    {
    sinkCall.setOutput( wrapOutput( output ) );
    }

  protected Output wrapOutput( Output output )
    {
    try
      {
      return scheme.sinkWrap( flowProcess, output );
      }
    catch( IOException exception )
      {
      throw new TapException( "could not wrap scheme", exception );
      }
    }

  /** Need to defer preparing the scheme till after the fields have been resolved */
  protected void prepare()
    {
    try
      {
      scheme.sinkPrepare( flowProcess, sinkCall );
      }
    catch( IOException exception )
      {
      throw new TapException( "could not prepare scheme", exception );
      }

    prepared = true;
    }

  @Override
  public void add( TupleEntry tupleEntry )
    {
    if( !prepared )
      prepare();

    super.add( tupleEntry );
    }

  @Override
  public void add( Tuple tuple )
    {
    if( !prepared ) // this is unfortunate
      prepare();

    super.add( tuple );
    }

  @Override
  protected void collect( TupleEntry tupleEntry ) throws IOException
    {
    sinkCall.setOutgoingEntry( tupleEntry );

    try
      {
      scheme.sink( flowProcess, sinkCall );
      }
    catch( Exception exception )
      {
      throw new TupleException( "unable to sink into output identifier: " + loggableIdentifier.get(), exception );
      }
    }

  @Override
  public void close()
    {
    try
      {
      if( sinkCall == null )
        return;

      try
        {
        if( prepared )
          scheme.sinkCleanup( flowProcess, sinkCall );
        }
      catch( IOException exception )
        {
        throw new TupleException( "unable to cleanup sink for output identifier: " + loggableIdentifier.get(), exception );
        }
      }
    finally
      {
      try
        {
        if( getOutput() instanceof Flushable )
          ( (Flushable) getOutput() ).flush();
        }
      catch( IOException exception )
        {
        // do nothing
        }

      try
        {
        if( getOutput() instanceof Closeable )
          ( (Closeable) getOutput() ).close();
        }
      catch( IOException exception )
        {
        // do nothing
        }

      super.close();
      }
    }
  }
