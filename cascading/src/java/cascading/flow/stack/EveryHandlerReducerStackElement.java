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

import cascading.flow.FlowElement;
import cascading.flow.Scope;
import cascading.pipe.Every;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
class EveryHandlerReducerStackElement extends ReducerStackElement
  {
  private final Every.EveryHandler everyHandler;

  public EveryHandlerReducerStackElement( StackElement previous, Scope incomingScope, JobConf jobConf, Tap trap, Every.EveryHandler everyHandler )
    {
    super( previous, incomingScope, jobConf, trap );
    this.everyHandler = everyHandler;
    }

  public FlowElement getFlowElement()
    {
    return null;
    }

  protected Fields resolveIncomingOperationFields()
    {
    return everyHandler.getEvery().resolveIncomingOperationFields( incomingScope );
    }

  public void collect( Tuple tuple )
    {
    operateEveryHandler( getGroupingTupleEntry( tuple ) );
    }

  private void operateEveryHandler( TupleEntry keyEntry )
    {
    try
      {
      everyHandler.complete( keyEntry, next );
      }
    catch( Exception exception )
      {
      handleException( exception, keyEntry );
      }
    }
  }
