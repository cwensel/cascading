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

package cascading.flow;

import java.io.IOException;
import java.util.Iterator;

import cascading.CascadingException;
import cascading.pipe.Each;
import cascading.pipe.Group;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
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
  private FlowMapperStackElement stackHead;
  private FlowMapperStackElement stackTail;


  public PushFlowMapperStack( JobConf jobConf )
    {
    step = (FlowStep) Util.deserializeBase64( jobConf.getRaw( FlowConstants.FLOW_STEP ) );
    currentSource = step.findCurrentSource( jobConf );

    if( LOG.isDebugEnabled() )
      LOG.debug( "map current source: " + currentSource );

    buildStack();
    }

  private void buildStack()
    {
    Scope incomingScope = step.getNextScope( currentSource );
    FlowElement operator = step.getNextFlowElement( incomingScope );

    stackTail = null;

    while( !( operator instanceof Group ) )
      {
      stackTail = new FlowMapperStackElement( stackTail, incomingScope, (Each) operator );

      incomingScope = step.getNextScope( operator );
      operator = step.getNextFlowElement( incomingScope );
      }

    if( !( operator instanceof Group ) )
      throw new IllegalStateException( "operator should be group, is instead: " + operator.getClass().getName() );

    Scope outgoingScope = step.getNextScope( operator ); // is always Group

    stackTail = new FlowMapperStackElement( stackTail, incomingScope, (Group) operator, outgoingScope );
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

  /**
   *
   */
  class FlowMapperStackElement implements FlowCollector
    {
    protected FlowMapperStackElement previous;
    protected FlowMapperStackElement next;

    private Scope incomingScope;
    private Each each;
    private Group group;
    private Scope outgoingScope;

    private Fields incomingFields;
    private Fields outgoingFields;

    private TupleEntry tupleEntry;
    private OutputCollector lastOutput;

    public FlowMapperStackElement( FlowMapperStackElement previous, Scope incomingScope, Each each )
      {
      this.previous = previous;
      this.incomingScope = incomingScope;
      this.each = each;
      }

    public FlowMapperStackElement( FlowMapperStackElement previous, Scope incomingScope, Group group, Scope outgoingScope )
      {
      this.previous = previous;
      this.incomingScope = incomingScope;
      this.group = group;
      this.outgoingScope = outgoingScope;
      }

    public FlowMapperStackElement resolveStack()
      {
      if( previous != null )
        return previous.setNext( this );

      return this;
      }

    private FlowMapperStackElement setNext( FlowMapperStackElement next )
      {
      this.next = next;

      if( previous != null )
        return previous.setNext( this );

      return this;
      }

    public void setLastOutput( OutputCollector lastOutput )
      {
      this.lastOutput = lastOutput;
      }

    private FlowElement getFlowElement()
      {
      if( each != null )
        return each;
      else
        return group;
      }

    public Fields resolveIncomingFields()
      {
      if( incomingFields == null )
        incomingFields = getFlowElement().resolveFields( incomingScope );

      return incomingFields;
      }

    public Scope getIncomingScope()
      {
      return incomingScope;
      }

    public Fields resolveOutgoingFields()
      {
      if( outgoingFields == null )
        outgoingFields = next.resolveIncomingFields();

      return outgoingFields;
      }

    private TupleEntry getTupleEntry( Tuple tuple )
      {
      if( tupleEntry == null )
        tupleEntry = new TupleEntry( resolveIncomingFields() );

      tupleEntry.setTuple( tuple );

      return tupleEntry;
      }

    public void collect( Tuple tuple )
      {
      if( tuple.isEmpty() )
        throw new FlowException( "may not collect an empty tuple" );

      if( each != null )
        operateEach( getTupleEntry( tuple ) );
      else
        operateGroup( getTupleEntry( tuple ) );
      }

    public void collect( Tuple key, Iterator tupleIterator )
      {
      throw new UnsupportedOperationException( "collect should never be called" );
      }

    private void operateEach( TupleEntry tupleEntry )
      {
      each.operate( next.getIncomingScope(), tupleEntry, next );
      }

    private void operateGroup( TupleEntry tupleEntry )
      {
      try
        {
        group.makeReduceGrouping( incomingScope, outgoingScope, tupleEntry, lastOutput );
        }
      catch( IOException exception )
        {
        throw new FlowException( "failed writing output", exception );
        }
      catch( Throwable throwable )
        {
        if( throwable instanceof CascadingException )
          throw (CascadingException) throwable;

        throw new FlowException( "internal error", throwable );
        }
      }

    @Override
    public String toString()
      {
      return each != null ? each.toString() : group.toString();
      }
    }
  }