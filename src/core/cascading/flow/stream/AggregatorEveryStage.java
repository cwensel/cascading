/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.flow.stream;

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.pipe.Every;
import cascading.pipe.OperatorException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Tuples;

/**
 *
 */
public class AggregatorEveryStage extends EveryStage<TupleEntry> implements Reducing<TupleEntry, TupleEntry>
  {
  private Aggregator aggregator;
  private Reducing reducing;

  public AggregatorEveryStage( FlowProcess flowProcess, Every every )
    {
    super( flowProcess, every );
    }

  @Override
  public void initialize()
    {
    super.initialize();

    aggregator = every.getAggregator();

    outputCollector = new TupleEntryCollector( outgoingScopes.get( 0 ).getDeclaredFields() )
    {
    @Override
    protected void collect( TupleEntry resultEntry ) throws IOException
      {
      Tuple outgoing = every.makeResult( outgoingSelector, incomingEntry, remainderFields, resultEntry, resultEntry.getTuple() );

      outgoingEntry.setTuple( outgoing );

      try
        {
        reducing.completeGroup( AggregatorEveryStage.this, outgoingEntry );
        }
      finally
        {
        Tuples.asModifiable( outgoing );
        }
      }
    };

    reducing = (Reducing) getNext();
    }

  @Override
  protected Fields getOutgoingSelector()
    {
    return outgoingScopes.get( 0 ).getOutGroupingSelector();
    }

  @Override
  public void startGroup( Duct previous, TupleEntry groupEntry )
    {
    operationCall.setGroup( groupEntry );
    operationCall.setArguments( null );  // zero it out
    operationCall.setOutputCollector( null ); // zero it out

    try
      {
      aggregator.start( flowProcess, operationCall );
      }
    catch( CascadingException exception )
      {
      handleException( exception, groupEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new OperatorException( every, "operator Every failed starting operation: " + every.getOperation(), throwable ), groupEntry );
      }

    reducing.startGroup( this, groupEntry );
    }

  @Override
  public void receive( Duct previous, TupleEntry tupleEntry )
    {
    try
      {
      argumentsEntry.setTuple( tupleEntry.selectTuple( argumentsSelector ) );
      operationCall.setArguments( argumentsEntry );

      aggregator.aggregate( flowProcess, operationCall );
      }
    catch( CascadingException exception )
      {
      handleException( exception, argumentsEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new OperatorException( every, "operator Every failed executing operation: " + every.getOperation(), throwable ), argumentsEntry );
      }

    next.receive( this, tupleEntry );
    }

  @Override
  public void completeGroup( Duct previous, TupleEntry incomingEntry )
    {
    this.incomingEntry = incomingEntry;
    operationCall.setArguments( null );
    operationCall.setOutputCollector( outputCollector );

    try
      {
      aggregator.complete( flowProcess, operationCall ); // collector calls next
      }
    catch( CascadingException exception )
      {
      handleException( exception, incomingEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new OperatorException( every, "operator Every failed completing operation: " + every.getOperation(), throwable ), incomingEntry );
      }
    }
  }
