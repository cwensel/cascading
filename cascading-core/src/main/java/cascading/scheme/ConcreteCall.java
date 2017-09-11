/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.scheme;

import cascading.tap.Tap;
import cascading.tuple.TupleEntry;

/**
 * The concrete base class for {@link SourceCall} and {@link SinkCall}.
 *
 * @param <Context>
 * @param <IO>
 */
public class ConcreteCall<Context, IO> implements SourceCall<Context, IO>, SinkCall<Context, IO>
  {
  Context context;
  TupleEntry entry;
  IO io;
  Tap tap;

  @Override
  public Context getContext()
    {
    return context;
    }

  @Override
  public void setContext( Context context )
    {
    this.context = context;
    }

  @Override
  public TupleEntry getOutgoingEntry()
    {
    return entry;
    }

  public void setOutgoingEntry( TupleEntry outgoingEntry )
    {
    this.entry = outgoingEntry;
    }

  @Override
  public TupleEntry getIncomingEntry()
    {
    return entry;
    }

  public void setIncomingEntry( TupleEntry incomingEntry )
    {
    this.entry = incomingEntry;
    }

  @Override
  public IO getInput()
    {
    return io;
    }

  public void setInput( IO input )
    {
    this.io = input;
    }

  @Override
  public IO getOutput()
    {
    return io;
    }

  public void setOutput( IO output )
    {
    this.io = output;
    }

  @Override
  public Tap getTap()
    {
    return tap;
    }

  public void setTap( Tap tap )
    {
    this.tap = tap;
    }
  }
