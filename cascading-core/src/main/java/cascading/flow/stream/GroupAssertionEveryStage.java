/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.operation.GroupAssertion;
import cascading.pipe.Every;
import cascading.pipe.OperatorException;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class GroupAssertionEveryStage extends EveryStage<TupleEntry> implements Reducing<TupleEntry, TupleEntry>
  {
  private GroupAssertion groupAssertion;
  private Reducing reducing;

  public GroupAssertionEveryStage( FlowProcess flowProcess, Every every )
    {
    super( flowProcess, every );
    }

  @Override
  protected Fields getIncomingPassThroughFields()
    {
    return incomingScopes.get( 0 ).getIncomingAggregatorPassThroughFields();
    }

  @Override
  protected Fields getIncomingArgumentsFields()
    {
    return incomingScopes.get( 0 ).getIncomingAggregatorArgumentFields();
    }

  @Override
  protected Fields getOutgoingSelector()
    {
    return outgoingScopes.get( 0 ).getOutGroupingSelector();
    }

  @Override
  public void initialize()
    {
    super.initialize();

    groupAssertion = every.getGroupAssertion();

    reducing = (Reducing) getNext();
    }

  @Override
  public void startGroup( Duct previous, TupleEntry groupEntry )
    {
    operationCall.setGroup( groupEntry );
    operationCall.setArguments( null );  // zero it out
    operationCall.setOutputCollector( null ); // zero it out

    try
      {
      groupAssertion.start( flowProcess, operationCall );
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
      argumentsEntry.setTuple( argumentsBuilder.makeResult( tupleEntry.getTuple(), null ) );
      operationCall.setArguments( argumentsEntry );

      groupAssertion.aggregate( flowProcess, operationCall );
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

    try
      {
      groupAssertion.doAssert( flowProcess, operationCall ); // collector calls next

      reducing.completeGroup( this, incomingEntry );
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
