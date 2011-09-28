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

package cascading.operation;

import java.beans.ConstructorProperties;
import java.util.Arrays;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Tuples;

/**
 * The Identity function simply passes incoming arguments back out again. Optionally argument fields can be renamed, and/or
 * coerced into specific types.
 */
public class Identity extends BaseOperation implements Function
  {
  /** Field types */
  private Class[] types = null;

  /**
   * Constructor Identity creates a new Identity instance that will pass the argument values to its output. Use this
   * constructor for a simple copy Pipe.
   */
  public Identity()
    {
    super( Fields.ARGS );
    }

  /**
   * Constructor Identity creates a new Identity instance that will coerce the values to the give types.
   *
   * @param types of type Class...
   */
  @ConstructorProperties({"types"})
  public Identity( Class... types )
    {
    super( Fields.ARGS );
    this.types = Arrays.copyOf( types, types.length );
    }

  /**
   * Constructor Identity creates a new Identity instance that will rename the argument fields to the given
   * fieldDeclaration.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public Identity( Fields fieldDeclaration )
    {
    super( fieldDeclaration ); // don't need to set size, default is ANY
    }

  /**
   * Constructor Identity creates a new Identity instance that will rename the argument fields to the given
   * fieldDeclaration, and coerce the values to the give types.
   *
   * @param fieldDeclaration of type Fields
   * @param types            of type Class...
   */
  @ConstructorProperties({"fieldDeclaration", "types"})
  public Identity( Fields fieldDeclaration, Class... types )
    {
    super( fieldDeclaration );
    this.types = Arrays.copyOf( types, types.length );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != types.length )
      throw new IllegalArgumentException( "fieldDeclaration and types must be the same size" );
    }

  /** @see Function#operate(cascading.flow.FlowProcess, FunctionCall) */
  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    TupleEntry input = functionCall.getArguments();
    TupleEntryCollector outputCollector = functionCall.getOutputCollector();

    if( types == null || types.length == 0 )
      outputCollector.add( input.getTuple() );
    else
      outputCollector.add( Tuples.coerce( input.getTuple(), types ) );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof Identity ) )
      return false;
    if( !super.equals( object ) )
      return false;

    Identity identity = (Identity) object;

    if( !Arrays.equals( types, identity.types ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( types != null ? Arrays.hashCode( types ) : 0 );
    return result;
    }
  }
