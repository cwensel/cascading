/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

import cascading.flow.FlowConstants;
import cascading.flow.FlowElement;
import cascading.flow.FlowMapperStack;
import cascading.flow.FlowStep;
import cascading.flow.Scope;
import cascading.pipe.Each;
import cascading.pipe.EndPipe;
import cascading.pipe.Group;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.util.Util;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

/**
 *
 */
public class PushFlowMapperStack extends FlowMapperStack
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( PushFlowMapperStack.class );

  /** Field step */
  private FlowStep step;
  /** Field currentSource */
  private Tap currentSource;

  private final JobConf jobConf;
  private FlowMapperStackElement stackHead;
  private FlowMapperStackElement stackTail;


  public PushFlowMapperStack( JobConf jobConf ) throws IOException
    {
    this.jobConf = jobConf;
    step = (FlowStep) Util.deserializeBase64( jobConf.getRaw( FlowConstants.FLOW_STEP ) );
    currentSource = step.findCurrentSource( jobConf );

    if( LOG.isDebugEnabled() )
      LOG.debug( "map current source: " + currentSource );

    buildStack();
    }

  private void buildStack() throws IOException
    {
    Scope incomingScope = step.getNextScope( currentSource );
    FlowElement operator = step.getNextFlowElement( incomingScope );

    stackTail = null;

    while( operator instanceof Each )
      {
      stackTail = new EachMapperStackElement( stackTail, incomingScope, (Each) operator );

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

      stackTail = new GroupMapperStackElement( stackTail, incomingScope, (Group) operator, outgoingScope );
      }
    else if( operator instanceof Tap )
      {
      useTapCollector = useTapCollector || ( (Tap) operator ).isUseTapCollector();

      stackTail = new TapMapperStackElement( stackTail, incomingScope, (Tap) operator, useTapCollector, jobConf );
      }
    else
      throw new IllegalStateException( "operator should be group or tap, is instead: " + operator.getClass().getName() );

    stackHead = stackTail.resolveStack();
    }

  public void map( WritableComparable key, Writable value, OutputCollector output ) throws IOException
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

    stackTail.setLastOutput( output );

    stackHead.collect( tuple );
    }

  public void close()
    {
    stackTail.close();
    }

  }