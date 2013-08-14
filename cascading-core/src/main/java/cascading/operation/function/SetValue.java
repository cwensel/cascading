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

package cascading.operation.function;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.util.Arrays;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class SetValue is a utility {@link Function} that allows for a Tuple value to be returned based on the outcome
 * of a given {@link Filter} operation.
 * <p/>
 * There are only two possible values, either {@link Filter#isRemove(cascading.flow.FlowProcess, cascading.operation.FilterCall)}
 * returns {@code true} or {@code false}.
 * <p/>
 * If {@code false} is returned, most commonly the {@link Filter} passed and the Tuple should be kept. SetValue will then return
 * the first value in the given values array, by default {@code true}. If the Filter returns {@code true}, the second
 * value in the values array will be returned, by default {@code false}.
 * <p/>
 */
public class SetValue extends BaseOperation implements Function
  {
  /** Field filter */
  private final Filter filter;
  /** Field values */
  private Tuple[] values = new Tuple[]{new Tuple( true ), new Tuple( false )};

  /**
   * Constructor SetValue creates a new SetValue instance.
   *
   * @param fieldDeclaration of type Fields
   * @param filter           of type Filter
   */
  @ConstructorProperties({"fieldDeclaration", "filter"})
  public SetValue( Fields fieldDeclaration, Filter filter )
    {
    super( fieldDeclaration );
    this.filter = filter;

    verify();
    }

  /**
   * Constructor SetValue creates a new SetValue instance.
   *
   * @param fieldDeclaration of type Fields
   * @param filter           of type Filter
   * @param firstValue       of type Serializable
   * @param secondValue      of type Serializable
   */
  @ConstructorProperties({"fieldDeclaration", "filter", "firstValue", "secondValue"})
  public SetValue( Fields fieldDeclaration, Filter filter, Serializable firstValue, Serializable secondValue )
    {
    super( fieldDeclaration );
    this.filter = filter;
    this.values = new Tuple[]{new Tuple( firstValue ), new Tuple( secondValue )};

    verify();
    }

  public Serializable getFirstValue()
    {
    return (Serializable) values[ 0 ].getObject( 0 );
    }

  public Serializable getSecondValue()
    {
    return (Serializable) values[ 1 ].getObject( 0 );
    }

  private void verify()
    {
    if( fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field, was " + fieldDeclaration.print() );

    if( filter == null )
      throw new IllegalArgumentException( "filter may not be null" );

    if( values == null || values.length != 2 )
      throw new IllegalArgumentException( "values argument must contain two values" );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall operationCall )
    {
    filter.prepare( flowProcess, operationCall );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    boolean isRemove = !filter.isRemove( flowProcess, (FilterCall) functionCall );

    int pos = isRemove ? 0 : 1;

    functionCall.getOutputCollector().add( values[ pos ] );
    }

  @Override
  public void cleanup( FlowProcess flowProcess, OperationCall operationCall )
    {
    filter.cleanup( flowProcess, operationCall );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof SetValue ) )
      return false;
    if( !super.equals( object ) )
      return false;

    SetValue setValue = (SetValue) object;

    if( filter != null ? !filter.equals( setValue.filter ) : setValue.filter != null )
      return false;
    if( !Arrays.equals( values, setValue.values ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( filter != null ? filter.hashCode() : 0 );
    result = 31 * result + ( values != null ? Arrays.hashCode( values ) : 0 );
    return result;
    }
  }