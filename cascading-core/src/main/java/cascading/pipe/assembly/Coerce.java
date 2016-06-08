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

package cascading.pipe.assembly;

import java.beans.ConstructorProperties;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * Class Coerce is a {@link SubAssembly} that will coerce all incoming {@link cascading.tuple.Tuple} values to
 * the given types.
 * <p/>
 * If the given type is a primitive ({@code long}), and the tuple value is null, {@code 0} is returned.
 * If the type is an Object ({@code java.lang.Long}), and the tuple value is {@code null}, {@code null} is returned.
 * <p/>
 * Coerce encapsulates the {@link Identity} function.
 * <p/>
 * Note if the resolved coerceFields size does not equal the number of given types there will be a
 * runtime error during execution.
 *
 * @see cascading.pipe.SubAssembly
 * @see cascading.operation.Identity
 */
public class Coerce extends SubAssembly
  {
  /**
   * Constructor Coerce creates a new Coerce instance that will coerce all input Tuple values.
   * <p/>
   * Note if the resolved coerceFields size does not equal the number of given types there will be a
   * runtime error during execution. Declaring the fields that must be coerced is a suggested practice.
   *
   * @param previous of type Pipe
   * @param types    of type Class...
   */
  @ConstructorProperties({"previous", "types"})
  public Coerce( Pipe previous, Class... types )
    {
    super( previous );

    if( types.length == 0 )
      throw new IllegalArgumentException( "given types array may not be zero length" );

    setTails( new Each( previous, new Identity( types ) ) );
    }

  /**
   * Constructor Coerce creates a new Coerce instance that will only coerce the given coerceFields Tuple values.
   * <p/>
   * Note the resulting output Tuple will contain all the original incoming Fields.
   * <p/>
   * Also note if the resolved coerceFields size does not equal the number of given types there will be a
   * runtime error during execution.
   *
   * @param previous     of type Pipe
   * @param coerceFields of type Fields
   * @param types        of type Class...
   */
  @ConstructorProperties({"previous", "coerceFields", "types"})
  public Coerce( Pipe previous, Fields coerceFields, Class... types )
    {
    super( previous );

    if( coerceFields == null )
      throw new IllegalArgumentException( "coerceFields may not be null" );

    if( types.length == 0 )
      throw new IllegalArgumentException( "given types array may not be zero length" );

    setTails( new Each( previous, coerceFields, new Identity( types ), Fields.REPLACE ) );
    }

  /**
   * Constructor Coerce creates a new Coerce instance that will only coerce the given coerceFields Tuple values.
   * <p/>
   * The given {@code coerceFields} instance must contain field type information, otherwise an
   * {@link IllegalArgumentException} will be thrown.
   * <p/>
   * Note the resulting output Tuple will contain all the original incoming Fields.
   *
   * @param previous     of type Pipe
   * @param coerceFields of type Fields
   */
  @ConstructorProperties({"previous", "coerceFields"})
  public Coerce( Pipe previous, Fields coerceFields )
    {
    super( previous );

    if( coerceFields == null )
      throw new IllegalArgumentException( "coerceFields may not be null" );

    if( !coerceFields.hasTypes() )
      throw new IllegalArgumentException( "coerceFields must have field types declared" );

    setTails( new Each( previous, coerceFields, new Identity( coerceFields ), Fields.REPLACE ) );

    if( coerceFields.getTypes().length == 0 )
      throw new IllegalArgumentException( "number of types must not be zero" );
    }
  }
