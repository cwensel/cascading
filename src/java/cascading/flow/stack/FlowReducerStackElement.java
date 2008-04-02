/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.flow.stack;

import java.util.Iterator;

import cascading.flow.FlowCollector;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.Scope;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
abstract class FlowReducerStackElement implements FlowCollector
  {
  protected FlowReducerStackElement previous;
  protected FlowReducerStackElement next;

  protected Scope incomingScope; // used by everything but group
  protected Fields outGroupingFields;

  private TupleEntry groupingTupleEntry;
  protected OutputCollector lastOutput;
  private Fields incomingFields;
  private TupleEntry tupleEntry;

  FlowReducerStackElement( FlowReducerStackElement previous, Scope incomingScope )
    {
    this.previous = previous;
    this.incomingScope = incomingScope;
    }

  protected FlowReducerStackElement( Fields outGroupingFields )
    {
    this.outGroupingFields = outGroupingFields;
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

  public abstract FlowElement getFlowElement();

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

  protected Fields resolveIncomingOperationFields()
    {
    return getFlowElement().resolveIncomingOperationFields( incomingScope );
    }

  public void collect( Tuple key, Iterator values )
    {
    throw new FlowException( "no next stack element" );
    }

  public void collect( Tuple tuple )
    {
    throw new FlowException( "no next stack element" );
    }

  public Fields getOutGroupingFields()
    {
    if( outGroupingFields == null )
      outGroupingFields = incomingScope.getOutGroupingFields();

    return outGroupingFields;
    }

  protected TupleEntry getGroupingTupleEntry( Tuple tuple )
    {
    if( groupingTupleEntry == null )
      groupingTupleEntry = new TupleEntry( getOutGroupingFields() );

    groupingTupleEntry.setTuple( tuple );

    return groupingTupleEntry;
    }

  protected TupleEntry getTupleEntry( Tuple tuple )
    {
    if( tupleEntry == null )
      tupleEntry = new TupleEntry( resolveIncomingFields() );

    tupleEntry.setTuple( tuple );

    return tupleEntry;
    }

  public void close()
    {
    }

  }
