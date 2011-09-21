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

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;

/**
 *
 */
public class TupleEntrySchemeCollector<O> extends TupleEntryCollector
  {
  private final FlowProcess flowProcess;
  private final Scheme scheme;

  protected final SinkCall sinkCall;

  public TupleEntrySchemeCollector( FlowProcess flowProcess, Scheme scheme )
    {
    super( Fields.asDeclaration( scheme.getSinkFields() ) );
    this.flowProcess = flowProcess;
    this.scheme = scheme;

    this.sinkCall = new SinkCall();
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
