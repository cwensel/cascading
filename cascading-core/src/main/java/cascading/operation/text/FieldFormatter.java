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

package cascading.operation.text;

import java.beans.ConstructorProperties;
import java.util.Formatter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class FieldFormatter formats the values in a Tuple with a given format and stuffs the result into a new field.
 * <p/>
 * This function uses the {@link Formatter} class for formatting the argument tuple values into a new string.
 */
public class FieldFormatter extends BaseOperation<Tuple> implements Function<Tuple>
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "formatted";

  /** Field format */
  private String format = null;

  /**
   * Constructor FieldJoiner creates a new FieldFormatter instance using the default field name "formatted".
   *
   * @param format of type String
   */
  @ConstructorProperties({"format"})
  public FieldFormatter( String format )
    {
    super( new Fields( FIELD_NAME ) );
    this.format = format;
    }

  /**
   * Constructor FieldJoiner creates a new FieldFormatter instance.
   *
   * @param fieldDeclaration of type Fields
   * @param format           of type String
   */
  @ConstructorProperties({"fieldDeclaration", "format"})
  public FieldFormatter( Fields fieldDeclaration, String format )
    {
    super( fieldDeclaration );
    this.format = format;

    if( fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field name, got " + fieldDeclaration.print() );
    }

  /**
   * Method getFormat returns the format of this FieldFormatter object.
   *
   * @return the format (type String) of this FieldFormatter object.
   */
  public String getFormat()
    {
    return format;
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Tuple> operationCall )
    {
    operationCall.setContext( Tuple.size( 1 ) );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Tuple> functionCall )
    {
    functionCall.getContext().set( 0, functionCall.getArguments().getTuple().format( format ) );
    functionCall.getOutputCollector().add( functionCall.getContext() );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof FieldFormatter ) )
      return false;
    if( !super.equals( object ) )
      return false;

    FieldFormatter that = (FieldFormatter) object;

    if( format != null ? !format.equals( that.format ) : that.format != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( format != null ? format.hashCode() : 0 );
    return result;
    }
  }