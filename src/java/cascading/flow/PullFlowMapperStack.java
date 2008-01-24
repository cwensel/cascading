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

import cascading.pipe.Each;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.util.Util;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

/**
 *
 */
public class PullFlowMapperStack extends FlowMapperStack
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( PullFlowMapperStack.class );

  /** Field step */
  private FlowStep step;
  /** Field currentSource */
  private Tap currentSource;


  public PullFlowMapperStack( JobConf jobConf )
    {
    step = (FlowStep) Util.deserializeBase64( jobConf.getRaw( FlowConstants.FLOW_STEP ) );
    currentSource = step.findCurrentSource( jobConf );

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

    Scope incomingScope = step.getNextScope( currentSource );
    FlowElement operator = step.getNextFlowElement( incomingScope );

    TupleEntryCollector valuesIterable = new TupleEntryCollector( operator.resolveFields( incomingScope ), tuple );

    while( operator != step.group )
      {
      incomingScope = step.getNextScope( operator );

      if( LOG.isDebugEnabled() )
        LOG.debug( "map outgoing fields: " + operator.resolveFields( incomingScope ) );

      // resolveIncoming fields actuall returns the actual fields expected or returned by the given operator
      TupleEntryCollector outgoingCollector = new TupleEntryCollector( operator.resolveFields( incomingScope ) );

      // todo: this is very innefficient. need to chain the output iterators so values are dumped immediately
      ( (Each) operator ).operate( incomingScope, valuesIterable.iterator(), outgoingCollector.iterator() );

      if( LOG.isDebugEnabled() )
        {
        for( TupleEntry outputTuple : outgoingCollector )
          LOG.debug( "map operator: [" + operator + "] map entry: " + outputTuple );
        }

      if( outgoingCollector.isEmpty() )
        return;

      valuesIterable = outgoingCollector;
      operator = step.getNextFlowElement( incomingScope );
      }

    if( LOG.isDebugEnabled() )
      LOG.debug( "map group: " + step.group );

    Scope outgoingScope = step.getNextScope( step.getNextFlowElement( incomingScope ) );
    // output all results
    for( TupleEntry outputTuple : valuesIterable )
      {
      // get a copy of the groupby keys and pass as the reducer key
      // this duplicates values in both the key and value
      // TODO: optimize, don't need to do a pos lookup every time
      Tuple[] group = step.group.makeReduceGrouping( incomingScope, outgoingScope, outputTuple );

      if( LOG.isDebugEnabled() )
        {
        LOG.debug( "map group keys: " + group[ 0 ].print() );
        LOG.debug( "map group values: " + group[ 1 ].print() );
        }

      output.collect( group[ 0 ], group[ 1 ] );
      }
    }

  }
