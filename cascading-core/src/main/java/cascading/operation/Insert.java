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

package cascading.operation;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/** Class Insert adds literal values to the Tuple stream. */
public class Insert extends BaseOperation implements Function
  {
  /** Field values */
  private final Tuple values;

  /**
   * Constructor Insert creates a new Insert instance with the given fields and values.
   *
   * @param fieldDeclaration of type Fields
   * @param values           of type Object...
   */
  @ConstructorProperties({"fieldDeclaration", "values"})
  public Insert( Fields fieldDeclaration, Object... values )
    {
    super( 0, fieldDeclaration );
    this.values = new Tuple( values );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != values.length )
      throw new IllegalArgumentException( "fieldDeclaration must be the same size as the given values" );
    }

  public Tuple getValues()
    {
    return new Tuple( values );
    }

  /** @see Function#operate(cascading.flow.FlowProcess, FunctionCall) */
  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    functionCall.getOutputCollector().add( values );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof Insert ) )
      return false;
    if( !super.equals( object ) )
      return false;

    Insert insert = (Insert) object;

    if( values != null ? !values.equals( insert.values ) : insert.values != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( values != null ? values.hashCode() : 0 );
    return result;
    }
  }
