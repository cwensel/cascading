/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.flow.stack;

import java.io.IOException;
import java.util.Iterator;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.Scope;
import cascading.tap.Tap;
import cascading.tap.TapCollector;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class TapReducerStackElement extends FlowReducerStackElement
  {
  private Tap sink;
  private TapCollector tapCollector;

  public TapReducerStackElement( FlowReducerStackElement previous, Scope incomingScope, Tap sink, boolean useTapCollector, JobConf jobConf ) throws IOException
    {
    super( previous, incomingScope );
    this.sink = sink;

    if( useTapCollector )
      this.tapCollector = sink.openForWrite( jobConf );
    }

  public FlowElement getFlowElement()
    {
    return sink;
    }

  public void collect( Tuple key, Iterator values )
    {
    operateSink( key, values );
    }

  public void collect( Tuple tuple )
    {
    operateSink( getTupleEntry( tuple ) );
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
      if( tapCollector != null )
        ( (Tap) getFlowElement() ).sink( tupleEntry.getFields(), tupleEntry.getTuple(), tapCollector );
      else
        ( (Tap) getFlowElement() ).sink( tupleEntry.getFields(), tupleEntry.getTuple(), lastOutput );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error: " + tupleEntry.getTuple().print(), throwable );
      }
    }

  @Override
  public void close()
    {
    if( tapCollector != null )
      tapCollector.close();
    }
  }
