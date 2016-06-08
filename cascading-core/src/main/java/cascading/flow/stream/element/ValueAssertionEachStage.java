/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream.element;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.operation.ValueAssertion;
import cascading.pipe.Each;
import cascading.pipe.OperatorException;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class ValueAssertionEachStage extends EachStage
  {
  private ValueAssertion valueAssertion;

  public ValueAssertionEachStage( FlowProcess flowProcess, Each each )
    {
    super( flowProcess, each );
    }

  @Override
  protected Fields getIncomingPassThroughFields()
    {
    return incomingScopes.get( 0 ).getIncomingFunctionPassThroughFields();
    }

  @Override
  protected Fields getIncomingArgumentsFields()
    {
    return incomingScopes.get( 0 ).getIncomingFunctionArgumentFields();
    }

  @Override
  public void initialize()
    {
    super.initialize();

    valueAssertion = each.getValueAssertion();
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    argumentsEntry.setTuple( argumentsBuilder.makeResult( incomingEntry.getTuple(), null ) );

    try
      {
      valueAssertion.doAssert( flowProcess, operationCall );

      next.receive( this, incomingEntry );
      }
    catch( CascadingException exception )
      {
      handleException( exception, argumentsEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new OperatorException( each, "operator Each failed executing operation", throwable ), argumentsEntry );
      }
    }
  }
