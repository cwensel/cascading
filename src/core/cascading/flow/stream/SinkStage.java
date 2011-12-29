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

package cascading.flow.stream;

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.StepCounters;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 *
 */
public class SinkStage extends ElementStage<TupleEntry, Void>
  {
  private final Tap sink;
  private TupleEntryCollector collector;

  public SinkStage( FlowProcess flowProcess, Tap sink )
    {
    super( flowProcess, sink );
    this.sink = sink;
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    // do not bind
    }

  @Override
  public void prepare()
    {
    try
      {
      collector = sink.openForWrite( flowProcess, getInput() );
      }
    catch( IOException exception )
      {
      throw new DuctException( "failed opening sink", exception );
      }
    }

  protected Object getInput()
    {
    return null;
    }

  @Override
  public void start( Duct previous )
    {
    // do nothing
    }

  @Override
  public void receive( Duct previous, TupleEntry tupleEntry )
    {
    try
      {
      collector.add( tupleEntry );
      flowProcess.increment( StepCounters.Tuples_Written, 1 );
      }
    catch( OutOfMemoryError error )
      {
      handleReThrowableException( "out of memory, try increasing task memory allocation", error );
      }
    catch( CascadingException exception )
      {
      handleException( exception, tupleEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new DuctException( "internal error: " + tupleEntry.getTuple().print(), throwable ), tupleEntry );
      }
    }

  @Override
  public void complete( Duct previous )
    {
    // do nothing
    }

  @Override
  public void cleanup()
    {
    try
      {
      if( collector != null )
        collector.close();

      collector = null;
      }
    finally
      {
      super.cleanup();
      }
    }
  }
