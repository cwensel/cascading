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
import java.lang.reflect.Type;
import java.util.Arrays;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.util.Util;

/**
 * The Identity function simply passes incoming arguments back out again. Optionally argument fields can be renamed, and/or
 * coerced into specific types.
 * <p/>
 * During coercion, if the given type is a primitive ({@code long}), and the tuple value is null, {@code 0} is returned.
 * If the type is an Object ({@code java.lang.Long}), and the tuple value is {@code null}, {@code null} is returned.
 */
public class Identity extends BaseOperation<Identity.Functor> implements Function<Identity.Functor>
  {
  /** Field types */
  private Type[] types = null;

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

    if( types.length == 0 )
      throw new IllegalArgumentException( "number of types must not be zero" );

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

    this.types = fieldDeclaration.getTypes();
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

  public Type[] getTypes()
    {
    return Util.copy( types );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Functor> operationCall )
    {
    Functor functor;

    if( types != null )
      {
      functor = new Functor()
      {
      Tuple result = Tuple.size( types.length );

      @Override
      public void operate( FunctionCall<Functor> functionCall )
        {
        TupleEntry input = functionCall.getArguments();
        TupleEntryCollector outputCollector = functionCall.getOutputCollector();

        outputCollector.add( input.getCoercedTuple( types, result ) );
        }
      };
      }
    else
      {
      functor = new Functor()
      {
      @Override
      public void operate( FunctionCall<Functor> functionCall )
        {
        TupleEntryCollector outputCollector = functionCall.getOutputCollector();

        outputCollector.add( functionCall.getArguments().getTuple() );
        }
      };
      operationCall.setContext( functor );
      }

    operationCall.setContext( functor );

    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Functor> functionCall )
    {
    functionCall.getContext().operate( functionCall );
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

  interface Functor
    {
    void operate( FunctionCall<Functor> functionCall );
    }
  }
