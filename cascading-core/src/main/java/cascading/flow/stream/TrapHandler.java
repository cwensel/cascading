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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.flow.StepCounters;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TrapHandler
  {
  private static final Logger LOG = LoggerFactory.getLogger( TrapHandler.class );

  static final Map<Tap, TupleEntryCollector> trapCollectors = new HashMap<Tap, TupleEntryCollector>();

  final FlowProcess flowProcess;
  final Tap trap;
  final String trapName;

  static TupleEntryCollector getTrapCollector( Tap trap, FlowProcess flowProcess )
    {
    TupleEntryCollector trapCollector = trapCollectors.get( trap );

    if( trapCollector == null )
      {
      try
        {
        trapCollector = flowProcess.openTrapForWrite( trap );
        trapCollectors.put( trap, trapCollector );
        }
      catch( IOException exception )
        {
        throw new DuctException( exception );
        }
      }

    return trapCollector;
    }

  static synchronized void closeTraps()
    {
    for( TupleEntryCollector trapCollector : trapCollectors.values() )
      {
      try
        {
        trapCollector.close();
        }
      catch( Exception exception )
        {
        // do nothing
        }
      }

    trapCollectors.clear();
    }

  public TrapHandler( FlowProcess flowProcess )
    {
    this.flowProcess = flowProcess;
    this.trap = null;
    this.trapName = null;
    }

  public TrapHandler( FlowProcess flowProcess, Tap trap, String trapName )
    {
    this.flowProcess = flowProcess;
    this.trap = trap;
    this.trapName = trapName;
    }

  protected void handleReThrowableException( String message, Throwable throwable )
    {
    LOG.error( message, throwable );

    if( throwable instanceof Error )
      throw (Error) throwable;
    else if( throwable instanceof RuntimeException )
      throw (RuntimeException) throwable;
    else
      throw new DuctException( message, throwable );
    }

  protected void handleException( Throwable exception, TupleEntry tupleEntry )
    {
    handleException( trapName, trap, exception, tupleEntry );
    }

  protected void handleException( String trapName, Tap trap, Throwable throwable, TupleEntry tupleEntry )
    {
    Throwable cause = throwable.getCause();

    if( cause instanceof OutOfMemoryError )
      handleReThrowableException( "caught OutOfMemoryException, will not trap, rethrowing", cause );

    if( trap == null )
      handleReThrowableException( "caught Throwable, no trap available, rethrowing", throwable );

    if( cause instanceof TapException && ( (TapException) cause ).getPayload() != null )
      {
      getTrapCollector( trap, flowProcess ).add( ( (TapException) cause ).getPayload() );
      }
    else if( tupleEntry != null )
      {
      getTrapCollector( trap, flowProcess ).add( tupleEntry );
      }
    else
      {
      LOG.error( "failure resolving tuple entry", throwable );
      throw new DuctException( "failure resolving tuple entry", throwable );
      }

    flowProcess.increment( StepCounters.Tuples_Trapped, 1 );

    LOG.warn( "exception trap on branch: '" + trapName + "', for " + Util.truncate( print( tupleEntry ), 75 ), throwable );
    }

  private String print( TupleEntry tupleEntry )
    {
    if( tupleEntry == null || tupleEntry.getFields() == null )
      return "[uninitialized]";
    else if( tupleEntry.getTuple() == null )
      return "fields: " + tupleEntry.getFields().printVerbose();
    else
      return "fields: " + tupleEntry.getFields().printVerbose() + " tuple: " + tupleEntry.getTuple().print();
    }
  }


