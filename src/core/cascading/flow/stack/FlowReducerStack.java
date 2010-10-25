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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowStep;
import cascading.flow.Scope;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tap.TempHfs;
import cascading.tuple.Tuple;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

/**
 *
 */
public class FlowReducerStack
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( FlowReducerStack.class );

  /** Field step */
  private final FlowStep step;
  /** Field jobConf */
  private final JobConf jobConf;
  /** Field flowSession */
  private final HadoopFlowProcess flowProcess;

  /** Field stackHead */
  private ReducerStackElement stackHead;
  /** Field stackTail */
  private ReducerStackElement stackTail;

  public FlowReducerStack( HadoopFlowProcess flowProcess ) throws IOException
    {
    this.flowProcess = flowProcess;
    this.jobConf = flowProcess.getJobConf();
    step = (FlowStep) Util.deserializeBase64( jobConf.getRaw( "cascading.flow.step" ) );

    // early versions of hadoop 0.19 instantiated this class with no intention of calling reduce()
    if( jobConf.getNumReduceTasks() == 0 )
      return;

    if( step.getGroup() == null )
      throw new IllegalStateException( "this step reducer should not be created, num reducers should be zero, found: " + jobConf.getNumReduceTasks() + ", in step: " + step.getStepName() );

    buildStack();

    stackTail.open();
    }

  private void buildStack() throws IOException
    {
    Set<Scope> previousScopes = step.getPreviousScopes( step.getGroup() );
    Scope nextScope = step.getNextScope( step.getGroup() );
    String trapName = ( (Pipe) step.getGroup() ).getName();
    Tap trap = step.getReducerTrap( trapName );

    stackTail = new GroupReducerStackElement( flowProcess, previousScopes, step.getGroup(), nextScope, nextScope.getOutGroupingFields(), trap );

    FlowElement operator = step.getNextFlowElement( nextScope );

    if( operator instanceof Every && !( (Every) operator ).isBuffer() )
      {
      List<Every.EveryHandler> allAggregators = new ArrayList<Every.EveryHandler>();
      Scope incomingScope = nextScope;

      stackTail = new EveryAllAggregatorReducerStackElement( stackTail, flowProcess, incomingScope, step.getReducerTraps(), allAggregators );

      while( operator instanceof Every && !( (Every) operator ).isBuffer() )
        {
        nextScope = step.getNextScope( operator );
        Every.EveryHandler everyHandler = ( (Every) operator ).getHandler( nextScope );

        allAggregators.add( everyHandler );

        trapName = ( (Pipe) operator ).getName();
        trap = step.getReducerTrap( trapName );
        stackTail = new EveryAggregatorReducerStackElement( stackTail, flowProcess, incomingScope, trap, everyHandler );
        incomingScope = nextScope;

        operator = step.getNextFlowElement( nextScope );
        }
      }
    else if( operator instanceof Every && ( (Every) operator ).isBuffer() )
      {
      Scope incomingScope = nextScope;

      while( operator instanceof Every && ( (Every) operator ).isBuffer() )
        {
        nextScope = step.getNextScope( operator );
        Every.EveryHandler everyHandler = ( (Every) operator ).getHandler( nextScope );

        trapName = ( (Pipe) operator ).getName();
        trap = step.getReducerTrap( trapName );
        stackTail = new EveryBufferReducerStackElement( stackTail, flowProcess, incomingScope, trap, everyHandler );
        incomingScope = nextScope;

        operator = step.getNextFlowElement( nextScope );
        }
      }

    while( operator instanceof Each )
      {
      trapName = ( (Pipe) operator ).getName();
      trap = step.getReducerTrap( trapName );
      stackTail = new EachReducerStackElement( stackTail, flowProcess, nextScope, trap, (Each) operator );

      nextScope = step.getNextScope( operator );
      operator = step.getNextFlowElement( nextScope );
      }

    boolean useTapCollector = false;

    useTapCollector = useTapCollector || ( (Tap) operator ).isWriteDirect();

    // no need for traps around intermediate files
    if( operator instanceof TempHfs )
      stackTail = new SinkReducerStackElement( stackTail, flowProcess, nextScope, (Tap) operator, useTapCollector );
    else
      stackTail = new SinkReducerStackElement( stackTail, flowProcess, nextScope, trapName, trap, (Tap) operator, useTapCollector );

    stackHead = (ReducerStackElement) stackTail.resolveStack();
    }

  public void reduce( Object key, Iterator values, OutputCollector output ) throws IOException
    {
    if( LOG.isTraceEnabled() )
      {
      LOG.trace( "reduce fields: " + stackHead.getOutGroupingFields() );
      LOG.trace( "reduce key: " + ( (Tuple) key ).print() );
      }

    stackTail.setLastOutput( output );

    try
      {
      stackHead.collect( (Tuple) key, values );
      }
    catch( StackException exception )
      {
      if( exception.getCause() instanceof Error )
        throw (Error) exception.getCause();

      if( exception.getCause() instanceof IOException )
        throw (IOException) exception.getCause();

      throw (RuntimeException) exception.getCause();
      }
    }

  public void close() throws IOException
    {
    stackHead.close();
    }
  }