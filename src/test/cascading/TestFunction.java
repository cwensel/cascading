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

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Integer> operationCall )
    {
    operationCall.setContext( 0 );
    }

  public void operate( FlowProcess flowProcess, FunctionCall<Integer> functionCall )
    {
    if( value == null )
      throw new RuntimeException( "function failed" );

    try
      {
      if( functionCall.getContext() == failon )
        throw new RuntimeException( "function failed" );
      }
    finally
      {
      functionCall.setContext( functionCall.getContext() + 1 );
      }

    functionCall.getOutputCollector().add( value );
    }

  @Override
  public void cleanup( FlowProcess flowProcess, OperationCall<Integer> operationCall )
    {
    if( value != null && value.get( 0 ) == null )
      throw new RuntimeException( "tuple was modified" );
    }

  @Override
  public boolean isSafe()
    {
    return isSafe;
    }
  }
