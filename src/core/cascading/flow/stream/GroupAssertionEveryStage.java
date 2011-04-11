/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
  public void initialize()
    {
    super.initialize();

    groupAssertion = every.getGroupAssertion();

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
      groupAssertion.start( flowProcess, operationCall );
      }
    catch( CascadingException exception )
      {
      handleException( exception, incomingEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new OperatorException( every, "operator Every failed starting operation: " + every.getOperation(), throwable ), incomingEntry );
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

      groupAssertion.aggregate( flowProcess, operationCall );
      }
    catch( CascadingException exception )
      {
      handleException( exception, incomingEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new OperatorException( every, "operator Every failed executing operation: " + every.getOperation(), throwable ), incomingEntry );
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
