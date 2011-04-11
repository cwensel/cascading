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

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.operation.ConcreteCall;
import cascading.pipe.Operator;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 *
 */
public abstract class OperatorStage<Incoming> extends ElementStage<Incoming, TupleEntry>
  {
  protected ConcreteCall operationCall;
  protected TupleEntry incomingEntry;
  protected Fields argumentsSelector;
  protected TupleEntry argumentsEntry;
  protected Fields remainderFields;
  protected Fields outgoingSelector;
  protected TupleEntry outgoingEntry;

  protected TupleEntryCollector outputCollector;

  public OperatorStage( FlowProcess flowProcess, FlowElement flowElement )
    {
    super( flowProcess, flowElement );
    }

  @Override
  public void initialize()
    {
    operationCall = new ConcreteCall( outgoingScopes.get( 0 ).getArgumentsDeclarator() );

    argumentsSelector = outgoingScopes.get( 0 ).getArgumentsSelector();
    remainderFields = outgoingScopes.get( 0 ).getRemainderFields();
    outgoingSelector = getOutgoingSelector();

    argumentsEntry = new TupleEntry( outgoingScopes.get( 0 ).getArgumentsDeclarator(), true );

    outgoingEntry = new TupleEntry( getOutgoingFields(), true );  // todo: simplify this

    operationCall.setArguments( argumentsEntry );
    }

  @Override
  public void prepare()
    {
    super.prepare(); // if fails, skip the this prepare

    ( (Operator) getFlowElement() ).getOperation().prepare( flowProcess, operationCall );
    }

  @Override
  public void complete( Duct previous )
    {
    try
      {
      ( (Operator) getFlowElement() ).getOperation().flush( flowProcess, operationCall );
      }
    finally
      {
      super.complete( previous );
      }
    }

  @Override
  public void cleanup()
    {
    try
      {
      ( (Operator) getFlowElement() ).getOperation().cleanup( flowProcess, operationCall );
      }
    finally
      {
      super.cleanup(); // guarantee this happens
      }
    }

  protected abstract Fields getOutgoingSelector();
  }
