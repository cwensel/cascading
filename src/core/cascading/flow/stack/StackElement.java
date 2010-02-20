/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stack;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowCollector;
import cascading.flow.FlowProcess;
import cascading.flow.StepCounters;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.TapCollector;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;

/** Class StackElement is the base class for Map and Reduce operation stacks. */
abstract class StackElement implements FlowCollector
  {

  private static Map<Tap, TapCollector> trapCollectors = new HashMap<Tap, TapCollector>();

  final FlowProcess flowProcess;
  private final Tap trap;
  StackElement previous;
  StackElement next;

  private static TapCollector getTrapCollector( Tap trap, JobConf jobConf )
    {
    TapCollector trapCollector = trapCollectors.get( trap );

    if( trapCollector == null )
      {
      try
        {
        jobConf = new JobConf( jobConf );

        int id = jobConf.getInt( "cascading.flow.step.id", 0 );
        String partname;

        if( jobConf.getBoolean( "mapred.task.is.map", true ) )
          partname = String.format( "-m-%05d-", id );
        else
          partname = String.format( "-r-%05d-", id );

        jobConf.set( "cascading.tapcollector.partname", "%s%spart" + partname + "%05d" );

        trapCollector = (TapCollector) trap.openForWrite( jobConf );
        trapCollectors.put( trap, trapCollector );
        }
      catch( IOException exception )
        {
        throw new StackException( exception );
        }
      }

    return trapCollector;
    }

  private static void closeTraps()
    {
    for( TapCollector trapCollector : trapCollectors.values() )
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

  public StackElement( FlowProcess flowProcess, Tap trap )
    {
    this.flowProcess = flowProcess;
    this.trap = trap;
    }

  public StackElement resolveStack()
    {
    if( previous != null )
      return previous.setNext( this );

    return this;
    }

  StackElement setNext( StackElement next )
    {
    this.next = next;

    if( previous != null )
      return previous.setNext( this );

    return this;
    }

  public abstract void prepare();

  public abstract void cleanup();

  public FlowProcess getFlowProcess()
    {
    return flowProcess;
    }

  public JobConf getJobConf()
    {
    return ( (HadoopFlowProcess) flowProcess ).getJobConf();
    }

  protected void handleException( Exception exception, TupleEntry tupleEntry )
    {
    handleException( trap, exception, tupleEntry );
    }

  protected void handleException( Tap trap, Exception exception, TupleEntry tupleEntry )
    {
    if( exception instanceof StackException )
      throw (StackException) exception;

    if( trap == null )
      throw new StackException( exception );

    getTrapCollector( trap, getJobConf() ).add( tupleEntry );
    getFlowProcess().increment( StepCounters.Tuples_Trapped, 1 );
    }

  public void open() throws IOException
    {
    prepare();

    // ok if skipped, don't open resources if failing
    if( previous != null )
      previous.open();
    }

  public void close() throws IOException
    {
    try
      {
      cleanup();
      }
    finally
      {
      try
        {
        // close if top of stack
        if( next == null )
          closeTraps();
        }
      finally
        {
        // need to try to close all open resources
        if( next != null )
          next.close();
        }
      }
    }

  }
