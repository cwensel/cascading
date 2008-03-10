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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cascading.CascadingException;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Util;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

/**
 *
 */
public class PushFlowReducerStack extends FlowReducerStack
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( PushFlowReducerStack.class );

  /** Field step */
  private FlowStep step;

  private final JobConf jobConf;
  private FlowReducerStackElement stackHead;
  private FlowReducerStackElement stackTail;

  public PushFlowReducerStack( JobConf jobConf )
    {
    this.jobConf = jobConf;
    step = (FlowStep) Util.deserializeBase64( jobConf.getRaw( FlowConstants.FLOW_STEP ) );

    buildStack();
    }

  private void buildStack()
    {
    Set<Scope> previousScopes = step.getPreviousScopes( step.group );
    Scope nextScope = step.getNextScope( step.group );

    stackTail = new FlowReducerStackElement( previousScopes, step.group, nextScope, nextScope.getOutGroupingFields() );

    FlowElement operator = step.getNextFlowElement( nextScope );

    if( operator instanceof Every )
      {
      List<Every.EveryHandler> everyHandlers = new ArrayList<Every.EveryHandler>();
      Scope incomingScope = nextScope;

      stackTail = new FlowReducerStackElement( stackTail, incomingScope, everyHandlers );

      while( operator instanceof Every )
        {
        nextScope = step.getNextScope( operator );
        Every.EveryHandler everyHandler = ( (Every) operator ).getHandler( nextScope );

        everyHandlers.add( everyHandler );

        stackTail = new FlowReducerStackElement( stackTail, incomingScope, everyHandler );
        incomingScope = nextScope;

        operator = step.getNextFlowElement( nextScope );
        }
      }

    while( operator instanceof Each )
      {
      stackTail = new FlowReducerStackElement( stackTail, nextScope, (Each) operator );

      nextScope = step.getNextScope( operator );
      operator = step.getNextFlowElement( nextScope );
      }

    stackTail = new FlowReducerStackElement( stackTail, nextScope, (Tap) operator );
    stackHead = (FlowReducerStackElement) stackTail.resolveStack();
    }


  public void reduce( WritableComparable key, Iterator values, OutputCollector output ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      {
      LOG.debug( "reduce fields: " + stackHead.getOutGroupingFields() );
      LOG.debug( "reduce key: " + ( (Tuple) key ).print() );
      }

    stackTail.setLastOutput( output );
    stackHead.collect( (Tuple) key, values );
    }

  /**
   *
   */
  class FlowReducerStackElement implements FlowCollector
    {
    protected FlowReducerStackElement previous;
    protected FlowReducerStackElement next;

    private Scope incomingScope; // used by everything but group
    private Set<Scope> incomingScopes; // used by group
    private FlowElement flowElement;
    private Scope thisScope; // used by group
    private Fields outGroupingFields;

    private List<Every.EveryHandler> everyHandlers;
    private Every.EveryHandler everyHandler;
    private TupleEntry groupingTupleEntry;
    private OutputCollector lastOutput;
    private Fields incomingFields;
    private TupleEntry tupleEntry;

    public FlowReducerStackElement( Set<Scope> incomingScopes, Group group, Scope thisScope, Fields outGroupingFields )
      {
      this.flowElement = group;
      this.incomingScopes = incomingScopes;
      this.thisScope = thisScope;
      this.outGroupingFields = outGroupingFields;
      }

    public FlowReducerStackElement( FlowReducerStackElement previous, Scope incomingScope, List<Every.EveryHandler> everyHandlers )
      {
      this.previous = previous;
      this.incomingScope = incomingScope;
      this.everyHandlers = everyHandlers;
      }

    public FlowReducerStackElement( FlowReducerStackElement previous, Scope incomingScope, Each each )
      {
      this.previous = previous;
      this.incomingScope = incomingScope;
      this.flowElement = each;
      }

    public FlowReducerStackElement( FlowReducerStackElement previous, Scope incomingScope, Tap sink )
      {
      this.previous = previous;
      this.incomingScope = incomingScope;
      this.flowElement = sink;
      }

    public FlowReducerStackElement( FlowReducerStackElement previous, Scope incomingScope, Every.EveryHandler everyHandler )
      {
      this.previous = previous;
      this.incomingScope = incomingScope;
      this.everyHandler = everyHandler;
      }

    public FlowReducerStackElement resolveStack()
      {
      if( previous != null )
        return previous.setNext( this );

      return this;
      }

    private FlowReducerStackElement setNext( FlowReducerStackElement next )
      {
      this.next = next;

      if( previous != null )
        return previous.setNext( this );

      return this;
      }

    public FlowElement getFlowElement()
      {
      return flowElement;
      }

    public Fields resolveIncomingFields()
      {
      if( incomingFields == null )
        incomingFields = getFlowElement().resolveFields( incomingScope );

      return incomingFields;
      }

    public void setLastOutput( OutputCollector lastOutput )
      {
      this.lastOutput = lastOutput;
      }

    public Scope getIncomingScope()
      {
      return incomingScope;
      }

    public Fields getOutGroupingFields()
      {
      if( outGroupingFields == null )
        outGroupingFields = incomingScope.getOutGroupingFields();

      return outGroupingFields;
      }

    private Fields resolveIncomingOperationFields()
      {
      if( everyHandlers != null )
        return next.resolveIncomingOperationFields();
      else if( everyHandler != null )
        return everyHandler.getEvery().resolveIncomingOperationFields( incomingScope );
      else
        return getFlowElement().resolveIncomingOperationFields( incomingScope );
      }

    public void collect( Tuple key, Iterator values )
      {
      if( flowElement instanceof Group )
        operateGroup( key, values );
      else if( everyHandlers != null )
        operateEveryHandlers( getGroupingTupleEntry( key ), values );
      else if( flowElement instanceof Each )
        operateEach( key, values );
      else if( flowElement instanceof Tap )
        operateSink( key, values );
      else
        throw new FlowException( "no next stack element" );
      }

    public void collect( Tuple tuple )
      {
      if( everyHandler != null )
        operateEveryHandler( getGroupingTupleEntry( tuple ) );
      else if( flowElement instanceof Each )
        operateEach( getTupleEntry( tuple ) );
      else if( flowElement instanceof Tap )
        operateSink( getTupleEntry( tuple ) );
      else
        throw new FlowException( "no next stack element" );
      }

    private TupleEntry getGroupingTupleEntry( Tuple tuple )
      {
      if( groupingTupleEntry == null )
        groupingTupleEntry = new TupleEntry( getOutGroupingFields() );

      groupingTupleEntry.setTuple( tuple );

      return groupingTupleEntry;
      }

    private TupleEntry getTupleEntry( Tuple tuple )
      {
      if( tupleEntry == null )
        tupleEntry = new TupleEntry( resolveIncomingFields() );

      tupleEntry.setTuple( tuple );

      return tupleEntry;
      }


    private void operateGroup( Tuple key, Iterator values )
      {
      // if a cogroup group instance...
      // an ungrouping iterator to partition the values back into a tuple so reduce stack can run
      // this can be one big tuple. the values iterator will have one Tuple of the format:
      // [ [key] [group1] [group2] ] where [groupX] == [ [...] [...] ...], a cogroup for each source
      // this can be nasty
      values = ( (Group) flowElement ).makeReduceValues( jobConf, incomingScopes, thisScope, key, values );

      values = new TupleEntryIterator( next.resolveIncomingOperationFields(), values );

      next.collect( key, values );
      }

    private void operateEveryHandlers( TupleEntry keyEntry, Iterator values )
      {
      for( Every.EveryHandler everyHandler : everyHandlers )
        everyHandler.start( keyEntry );

      while( values.hasNext() )
        {
        TupleEntry valueEntry = (TupleEntry) values.next();

        for( Every.EveryHandler handler : everyHandlers )
          handler.operate( valueEntry );
        }

      next.collect( keyEntry.getTuple() );
      }

    private void operateEveryHandler( TupleEntry keyEntry )
      {
      everyHandler.complete( keyEntry, next );
      }

    private void operateEach( Tuple key, Iterator values )
      {
      while( values.hasNext() )
        operateEach( (TupleEntry) values.next() );
      }

    private void operateEach( TupleEntry tupleEntry )
      {
      ( (Each) flowElement ).operate( next.getIncomingScope(), tupleEntry, next );
      }

    private void operateSink( Tuple key, Iterator values )
      {
      while( values.hasNext() )
        operateSink( (TupleEntry) values.next() );
      }

    private void operateSink( TupleEntry tupleEntry )
      {
      try
        {
        ( (Tap) flowElement ).sink( tupleEntry.getFields(), tupleEntry.getTuple(), lastOutput );
        }
      catch( Throwable throwable )
        {
        if( throwable instanceof CascadingException )
          throw (CascadingException) throwable;

        throw new FlowException( "internal error: " + tupleEntry.getTuple().print(), throwable );
        }
      }
    }
  }