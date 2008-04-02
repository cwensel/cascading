/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.flow.stack;

import java.io.IOException;

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
public class TapMapperStackElement extends FlowMapperStackElement
  {
  private Tap sink;
  private TapCollector tapCollector;

  public TapMapperStackElement( FlowMapperStackElement previous, Scope incomingScope, Tap sink, boolean useTapCollector, JobConf conf ) throws IOException
    {
    super( previous, incomingScope );
    this.sink = sink;

    if( useTapCollector )
      this.tapCollector = sink.openForWrite( conf );
    }

  protected FlowElement getFlowElement()
    {
    return sink;
    }

  @Override
  public void collect( Tuple tuple )
    {
    super.collect( tuple );

    operateSink( getTupleEntry( tuple ) );
    }

  private void operateSink( TupleEntry tupleEntry )
    {
    try
      {
      if( tapCollector != null )
        sink.sink( tupleEntry.getFields(), tupleEntry.getTuple(), tapCollector );
      else
        sink.sink( tupleEntry.getFields(), tupleEntry.getTuple(), lastOutput );
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
