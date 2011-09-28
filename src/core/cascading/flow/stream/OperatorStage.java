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
