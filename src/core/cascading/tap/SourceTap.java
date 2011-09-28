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

package cascading.tap;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * Class SourceTap is the base class for source only Taps. Some {@link Tap} instances may only be sources (as opposed
 * to being a sink). These types should subclass SourceTap for convenience.
 */
public abstract class SourceTap<Process extends FlowProcess, Config, Input, Output> extends Tap<Process, Config, Input, Output>
  {
  protected SourceTap()
    {
    }

  protected SourceTap( Scheme scheme )
    {
    super( scheme );
    }

  @Override
  public Fields getSinkFields()
    {
    throw new UnsupportedOperationException( "unable to sink tuple streams via a SourceTap instance" );
    }

  @Override
  public final boolean isSink()
    {
    return false;
    }

  /** @see Tap#deleteResource(Object) */
  public boolean deleteResource( Config conf ) throws IOException
    {
    throw new UnsupportedOperationException( "unable to delete files via a SourceTap instance" );
    }

  /** @see Tap#createResource(Object) */
  public boolean createResource( Config conf ) throws IOException
    {
    throw new UnsupportedOperationException( "unable to make dirs via a SourceTap instance" );
    }

  public TupleEntryIterator openForRead( Process flowProcess, Input input ) throws IOException
    {
    throw new UnsupportedOperationException( "unable to open for read via a SourceTap instance" );
    }

  public TupleEntryCollector openForWrite( Process flowProcess, Output output ) throws IOException
    {
    throw new UnsupportedOperationException( "unable to open for write via a SourceTap instance" );
    }
  }
