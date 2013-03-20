/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class TestFunction extends BaseOperation<Integer> implements Function<Integer>
  {
  int failon = -1;
  private Tuple value;
  private boolean isSafe = true;

  public TestFunction( Fields fieldDeclaration, Tuple value, boolean isSafe )
    {
    super( fieldDeclaration );
    this.value = value;
    this.isSafe = isSafe;
    }

  public TestFunction( Fields fieldDeclaration, Tuple value )
    {
    super( fieldDeclaration );
    this.value = value;
    }

  public TestFunction( Fields fieldDeclaration, Tuple value, int failon )
    {
    super( fieldDeclaration );
    this.value = value;
    this.failon = failon;
    }

  public TestFunction( Fields fieldDeclaration, Tuple value, int failon, boolean isSafe )
    {
    super( fieldDeclaration );
    this.value = value;
    this.failon = failon;
    this.isSafe = isSafe;
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Integer> operationCall )
    {
    operationCall.setContext( 0 );
    }

  public void operate( FlowProcess flowProcess, FunctionCall<Integer> functionCall )
    {
    if( value == null )
      throwIntentionalException();

    try
      {
      if( functionCall.getContext() == failon )
        throw new RuntimeException( "function failed intentionally on tuple number: " + failon );
      }
    finally
      {
      functionCall.setContext( functionCall.getContext() + 1 );
      }

    functionCall.getOutputCollector().add( value );
    }

  protected void throwIntentionalException()
    {
    throw new RuntimeException( "function failed intentionally" );
    }

  @Override
  public void cleanup( FlowProcess flowProcess, OperationCall<Integer> operationCall )
    {
    if( value != null && value.getObject( 0 ) == null )
      throw new RuntimeException( "tuple was modified" );
    }

  @Override
  public boolean isSafe()
    {
    return isSafe;
    }
  }
