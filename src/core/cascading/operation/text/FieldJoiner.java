/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/** Class FieldJoiner joins the values in a Tuple with a given delimiter and stuffs the result into a new field. */
public class FieldJoiner extends BaseOperation implements Function
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "joined";

  /** Field delimiter */
  private String delimiter = "\t";

  /**
   * Constructor FieldJoiner creates a new FieldJoiner instance.
   *
   * @param delimiter of type String
   */
  @ConstructorProperties({"delimiter"})
  public FieldJoiner( String delimiter )
    {
    this( new Fields( FIELD_NAME ) );
    this.delimiter = delimiter;
    }

  /**
   * Constructor FieldJoiner creates a new FieldJoiner instance.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public FieldJoiner( Fields fieldDeclaration )
    {
    super( fieldDeclaration );
    }

  /**
   * Constructor FieldJoiner creates a new FieldJoiner instance.
   *
   * @param fieldDeclaration of type Fields
   * @param delimiter        of type String
   */
  @ConstructorProperties({"fieldDeclaration", "delimiter"})
  public FieldJoiner( Fields fieldDeclaration, String delimiter )
    {
    super( fieldDeclaration );
    this.delimiter = delimiter;
    }

  /**
   * Method getFormat returns the delimiter of this FieldJoiner object.
   *
   * @return the delimiter (type String) of this FieldJoiner object.
   */
  public String getDelimiter()
    {
    return delimiter;
    }

  /** @see Function#operate(cascading.flow.FlowProcess, cascading.operation.FunctionCall) */
  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    functionCall.getOutputCollector().add( new Tuple( functionCall.getArguments().getTuple().toString( delimiter ) ) );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof FieldJoiner ) )
      return false;
    if( !super.equals( object ) )
      return false;

    FieldJoiner that = (FieldJoiner) object;

    if( delimiter != null ? !delimiter.equals( that.delimiter ) : that.delimiter != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( delimiter != null ? delimiter.hashCode() : 0 );
    return result;
    }
  }
