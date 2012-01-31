/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.ConcreteCall;
import cascading.scheme.Scheme;

/**
 *
 */
public class TupleEntrySchemeCollector<O> extends TupleEntryCollector
  {
  private final FlowProcess flowProcess;
  private final Scheme scheme;

  protected final ConcreteCall sinkCall;

  public TupleEntrySchemeCollector( FlowProcess flowProcess, Scheme scheme )
    {
    super( Fields.asDeclaration( scheme.getSinkFields() ) );
    this.flowProcess = flowProcess;
    this.scheme = scheme;

    this.sinkCall = new ConcreteCall();
    }

  public TupleEntrySchemeCollector( FlowProcess flowProcess, Scheme scheme, O output )
    {
    this( flowProcess, scheme );

    setOutput( output );
    }

  protected void setOutput( O output )
    {
    sinkCall.setOutput( output );
    }

  /**
   * Must be called within {@link cascading.tap.Tap#openForWrite(cascading.flow.FlowProcess, Object)}.
   * <p/>
   * Allows for an Output instance to be set before #sinkPrepare is called on the Scheme.
   *
   * @throws IOException
   */
  public void prepare() throws IOException
    {
    scheme.sinkPrepare( flowProcess, sinkCall );
    }

  @Override
  protected void collect( TupleEntry tupleEntry ) throws IOException
    {
    sinkCall.setOutgoingEntry( tupleEntry );

    scheme.sink( flowProcess, sinkCall );
    }

  @Override
  public void close()
    {
    try
      {
      if( sinkCall != null )
        scheme.sinkCleanup( flowProcess, sinkCall );
      }
    finally
      {
      super.close();
      }
    }
  }
