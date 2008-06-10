/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.FlowConstants;
import cascading.flow.FlowElement;
import cascading.flow.FlowStep;
import cascading.flow.Scope;
import cascading.pipe.Each;
import cascading.pipe.EndPipe;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.util.Util;
import org.apache.hadoop.io.WritableComparable;
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

  /** Field stackHead */
  private ReducerStackElement stackHead;
  /** Field stackTail */
  private ReducerStackElement stackTail;

  public FlowReducerStack( JobConf jobConf ) throws IOException
    {
    this.jobConf = jobConf;
    step = (FlowStep) Util.deserializeBase64( jobConf.getRaw( FlowConstants.FLOW_STEP ) );

    buildStack();
    }

  private void buildStack() throws IOException
    {
    Set<Scope> previousScopes = step.getPreviousScopes( step.group );
    Scope nextScope = step.getNextScope( step.group );
    Tap trap = step.getTrap( ( (Pipe) step.group ).getName() );

    stackTail = new GroupReducerStackElement( previousScopes, step.group, nextScope, nextScope.getOutGroupingFields(), jobConf, trap );

    FlowElement operator = step.getNextFlowElement( nextScope );

    if( operator instanceof Every )
      {
      List<Every.EveryHandler> everyHandlers = new ArrayList<Every.EveryHandler>();
      Scope incomingScope = nextScope;

      stackTail = new EveryHandlersReducerStackElement( stackTail, incomingScope, jobConf, step.traps, everyHandlers );

      while( operator instanceof Every )
        {
        nextScope = step.getNextScope( operator );
        Every.EveryHandler everyHandler = ( (Every) operator ).getHandler( nextScope );

        everyHandlers.add( everyHandler );

        trap = step.getTrap( ( (Pipe) operator ).getName() );
        stackTail = new EveryHandlerReducerStackElement( stackTail, incomingScope, jobConf, trap, everyHandler );
        incomingScope = nextScope;

        operator = step.getNextFlowElement( nextScope );
        }
      }

    while( operator instanceof Each )
      {
      trap = step.getTrap( ( (Pipe) operator ).getName() );
      stackTail = new EachReducerStackElement( stackTail, nextScope, jobConf, trap, (Each) operator );

      nextScope = step.getNextScope( operator );
      operator = step.getNextFlowElement( nextScope );
      }

    boolean useTapCollector = false;

    while( operator instanceof EndPipe )
      {
      useTapCollector = true;
      nextScope = step.getNextScope( operator );
      operator = step.getNextFlowElement( nextScope );
      }

    useTapCollector = useTapCollector || ( (Tap) operator ).isUseTapCollector();

    stackTail = new TapReducerStackElement( stackTail, nextScope, (Tap) operator, useTapCollector, jobConf );
    stackHead = (ReducerStackElement) stackTail.resolveStack();
    }

  public void reduce( WritableComparable key, Iterator values, OutputCollector output )
    {
    if( LOG.isDebugEnabled() )
      {
      LOG.debug( "reduce fields: " + stackHead.getOutGroupingFields() );
      LOG.debug( "reduce key: " + ( (Tuple) key ).print() );
      }

    stackTail.setLastOutput( output );

    try
      {
      stackHead.collect( (Tuple) key, values );
      }
    catch( StackException exception ) // unwrap
      {
      throw (RuntimeException) exception.getCause();
      }
    }

  public void close() throws IOException
    {
    stackTail.close();
    }
  }