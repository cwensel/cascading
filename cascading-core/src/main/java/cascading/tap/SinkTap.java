/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

/**
 * Class SinkTap is an optional base class for sink only Taps.
 * <p/>
 * Some {@link cascading.tap.Tap} instances may only be sinks (as opposed
 * to being a source). These types may subclass SinkTap for convenience or
 * set {@link #isSource()} to {@code false} in a custom Tap sub-class.
 */
public abstract class SinkTap<Config, Output> extends Tap<Config, Void, Output>
  {
  protected SinkTap()
    {
    }

  protected SinkTap( Scheme<Config, ?, Output, ?, ?> scheme )
    {
    super( (Scheme<Config, Void, Output, ?, ?>) scheme );
    }

  protected SinkTap( Scheme<Config, ?, Output, ?, ?> scheme, SinkMode sinkMode )
    {
    super( (Scheme<Config, Void, Output, ?, ?>) scheme, sinkMode );
    }

  @Override
  public Fields getSourceFields()
    {
    throw new UnsupportedOperationException( "unable to source tuple streams via a SinkTap instance" );
    }

  @Override
  public boolean prepareResourceForRead( Config conf ) throws IOException
    {
    throw new UnsupportedOperationException( "unable to prepare resource for read via a SinkTap instance" );
    }

  @Override
  public boolean isSource()
    {
    return false;
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Config> flowProcess, Config conf )
    {
    throw new UnsupportedOperationException( "unable to source tuple streams via a SinkTap instance" );
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Config> flowProcess, Void input ) throws IOException
    {
    throw new UnsupportedOperationException( "unable to open for read via a SinkTap instance" );
    }
  }