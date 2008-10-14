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
import java.util.Set;

import cascading.flow.FlowConstants;
import cascading.flow.FlowElement;
import cascading.flow.FlowStep;
import cascading.flow.Scope;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.Each;
import cascading.pipe.EndPipe;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

/**
 *
 */
public class FlowMapperStack
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( FlowMapperStack.class );

  /** Field step */
  private final FlowStep step;
  /** Field currentSource */
  private final Tap currentSource;
  /** Field jobConf */
  private final JobConf jobConf;
  /** Field flowSession */
  private final HadoopFlowProcess flowProcess;

  /** Field stack */
  private Stack stacks[];

  /** Class Stack is a simple holder for stack head and tails */
  private class Stack
    {
    /** Field stackHead */
    MapperStackElement head;
    /** Field stackTail */
    MapperStackElement tail;
    }

  public FlowMapperStack( HadoopFlowProcess flowProcess ) throws IOException
    {
    this.flowProcess = flowProcess;
    this.jobConf = flowProcess.getJobConf();
    step = (FlowStep) Util.deserializeBase64( jobConf.getRaw( FlowConstants.FLOW_STEP ) );

    // is set by the MultiInputSplit
    currentSource = (Tap) Util.deserializeBase64( jobConf.getRaw( FlowConstants.STEP_SOURCE ) );

    if( LOG.isDebugEnabled() )
      LOG.debug( "map current source: " + currentSource );

    buildStack();

    for( Stack stack : stacks )
      stack.tail.open();
    }

  private void buildStack() throws IOException
    {
    Set<Scope> incomingScopes = step.getNextScopes( currentSource );

    stacks = new Stack[incomingScopes.size()];

    int i = 0;

    for( Scope incomingScope : incomingScopes )
      {
      FlowElement operator = step.getNextFlowElement( incomingScope );

      stacks[ i ] = new Stack();

      stacks[ i ].tail = null;

      while( operator instanceof Each )
        {
        Tap trap = step.getTrap( ( (Pipe) operator ).getName() );
        stacks[ i ].tail = new EachMapperStackElement( stacks[ i ].tail, flowProcess, incomingScope, trap, (Each) operator );

        incomingScope = step.getNextScope( operator );
        operator = step.getNextFlowElement( incomingScope );
        }

      boolean useTapCollector = false;

      while( operator instanceof EndPipe )
        {
        useTapCollector = true;
        incomingScope = step.getNextScope( operator );
        operator = step.getNextFlowElement( incomingScope );
        }

      if( operator instanceof Group )
        {
        Scope outgoingScope = step.getNextScope( operator ); // is always Group

        Tap trap = step.getTrap( ( (Pipe) operator ).getName() );
        stacks[ i ].tail = new GroupMapperStackElement( stacks[ i ].tail, flowProcess, incomingScope, trap, (Group) operator, outgoingScope );
        }
      else if( operator instanceof Tap )
        {
        useTapCollector = useTapCollector || ( (Tap) operator ).isUseTapCollector();

        stacks[ i ].tail = new TapMapperStackElement( stacks[ i ].tail, flowProcess, incomingScope, (Tap) operator, useTapCollector );
        }
      else
        throw new IllegalStateException( "operator should be group or tap, is instead: " + operator.getClass().getName() );

      stacks[ i ].head = (MapperStackElement) stacks[ i ].tail.resolveStack();

      i++;
      }
    }

  public void map( Object key, Object value, OutputCollector output ) throws IOException
    {
    for( int i = 0; i < stacks.length; i++ )
      {
      Tuple tuple = currentSource.source( key, value );

      if( LOG.isDebugEnabled() )
        {
        if( key instanceof Tuple )
          LOG.debug( "map key: " + ( (Tuple) key ).print() );
        else
          LOG.debug( "map key: [" + key + "]" );

        LOG.debug( "map value: " + tuple.print() );
        }

      stacks[ i ].tail.setLastOutput( output );

      try
        {
        stacks[ i ].head.collect( tuple );
        }
      catch( StackException exception )
        {
        if( exception.getCause() instanceof IOException )
          throw (IOException) exception.getCause();

        throw (RuntimeException) exception.getCause();
        }
      }
    }

  public void close() throws IOException
    {
    for( int i = 0; i < stacks.length; i++ )
      {
      stacks[ i ].tail.cleanup();
      stacks[ i ].tail.close();
      }
    }
  }