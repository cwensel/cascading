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

import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Util;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

/**
 *
 */
public class PullFlowReducerStack extends FlowReducerStack
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( PullFlowReducerStack.class );

  /** Field step */
  private FlowStep step;
  /** Field currentSink */
  private Tap currentSink;

  public PullFlowReducerStack( JobConf jobConf )
    {
    step = (FlowStep) Util.deserializeBase64( jobConf.getRaw( FlowConstants.FLOW_STEP ) );
    currentSink = step.sink;
    }

  public void reduce( WritableComparable key, Iterator values, OutputCollector output ) throws IOException
    {
    Scope nextScope = step.getNextScope( step.group );
    FlowElement operator = step.getNextFlowElement( nextScope );
    TupleEntry groupEntry = new TupleEntry( nextScope.getOutGroupingFields(), (Tuple) key );

    if( LOG.isDebugEnabled() )
      {
      LOG.debug( "reduce key: " + ( (Tuple) key ).print() );
      LOG.debug( "reduce group: " + nextScope.getOutGroupingFields() );
      }

    // if a cogroup group instance...
    // an ungrouping iterator to partition the values back into a tuple so reduce stack can run
    // this can be one big tuple. the values iterator will have one Tuple of the format:
    // [ [key] [group1] [group2] ] where [groupX] == [ [...] [...] ...], a cogroup for each source
    // this can be nasty
    values = step.group.makeReduceValues( key, values );

    values = new TupleEntryIterator( operator.resolveIncomingOperationFields( nextScope ), values );

    if( operator instanceof Every )
      {
      List<Every.EveryHandler> everyHandlers = new ArrayList<Every.EveryHandler>();

      while( operator instanceof Every )
        {
        nextScope = step.getNextScope( operator );
        everyHandlers.add( ( (Every) operator ).getHandler( nextScope ) );
        operator = step.getNextFlowElement( nextScope );
        }

      for( Every.EveryHandler everyHandler : everyHandlers )
        everyHandler.start();

      while( values.hasNext() )
        {
        TupleEntry valueEntry = (TupleEntry) values.next();

        for( Every.EveryHandler handler : everyHandlers )
          {
          if( LOG.isDebugEnabled() )
            {
            LOG.debug( "reduce every operator: " + handler );
            LOG.debug( "reduce every group fields: " + groupEntry.getFields() );
            LOG.debug( "reduce every incoming fields: " + valueEntry.getFields() );
            }

          handler.operate( valueEntry );
          }
        }

      // output of operation must be of groupings
      values = new TupleEntryCollector( groupEntry.getFields(), (Tuple) key ).iterator();

      for( Every.EveryHandler handler : everyHandlers )
        {
        TupleEntryCollector outputEntryCollector = new TupleEntryCollector( handler.outgoingScope.getOutGroupingFields() );

        handler.complete( values, outputEntryCollector.iterator() );
        values = outputEntryCollector.iterator();
        }
      }

    while( operator != step.sink )
      {
      Each each = (Each) operator;
      if( LOG.isDebugEnabled() )
        {
        LOG.debug( "reduce each operator: " + each );
        LOG.debug( "reduce each incoming fields: " + nextScope.getOutValuesSelector() );
        }

      nextScope = step.getNextScope( each );
      TupleEntryCollector outputEntryCollector = new TupleEntryCollector( each.resolveFields( nextScope ) );

      if( LOG.isDebugEnabled() )
        LOG.debug( "reduce each outgoing fields: " + each.resolveFields( nextScope ) );

      each.operate( nextScope, values, outputEntryCollector.iterator() );

      values = outputEntryCollector.iterator();
      operator = step.getNextFlowElement( nextScope );
      }

    while( values.hasNext() )
      {
      TupleEntry value = (TupleEntry) values.next();
      currentSink.sink( value.getFields(), value.getTuple(), output );
      }
    }

  }
