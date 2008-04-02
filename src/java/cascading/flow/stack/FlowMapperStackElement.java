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
abstract class FlowMapperStackElement implements FlowCollector
  {
  protected FlowMapperStackElement previous;
  protected FlowMapperStackElement next;

  protected Scope incomingScope;

  private Fields incomingFields;
  private Fields outgoingFields;

  private TupleEntry tupleEntry;
  protected OutputCollector lastOutput;

  protected FlowMapperStackElement( FlowMapperStackElement previous, Scope incomingScope )
    {
    this.previous = previous;
    this.incomingScope = incomingScope;
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

  protected abstract FlowElement getFlowElement();

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

  protected TupleEntry getTupleEntry( Tuple tuple )
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
    }

  public void collect( Tuple key, Iterator tupleIterator )
    {
    throw new UnsupportedOperationException( "collect should never be called" );
    }

  @Override
  public String toString()
    {
    return getFlowElement().toString();
    }

  public void close()
    {
    }
  }
