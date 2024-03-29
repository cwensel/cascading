/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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
import java.lang.reflect.Type;

import cascading.flow.FlowProcess;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;

/**
 * Class SumByLocally is used to sum values associated with duplicate keys in a tuple stream.
 * <p>
 * It is important to read about the {@link AggregateByLocally} base class and the subsequent limitation of this class
 * in relation to the {@link SumBy} subassembly.
 * <p>
 * If all the values to be summed are all {@code null}, the result value is a function of how null is coerced by the
 * given {@code sumType}. If a primitive type, {@code 0} will be returned. Otherwise {@code null}.
 * <p>
 * This SubAssembly uses the {@link SumByLocally.SumPartials} {@link Functor} to sum field values.
 * <p>
 * The {@code threshold} value tells the underlying SumPartials functions how many unique key sums to accumulate
 * in the LRU cache, before emitting the least recently used entry. This accumulation is
 * bounded by the size of your map task JVM and the typical size of each group key.
 * <p>
 * By default, either the value of {@link AggregateByLocallyProps#AGGREGATE_LOCALLY_BY_CAPACITY} System property
 * or {@link AggregateByLocallyProps#AGGREGATE_LOCALLY_BY_DEFAULT_CAPACITY} will be used.
 *
 * @see AggregateBy
 */
public class SumByLocally extends AggregateByLocally
  {
  /**
   * Class SumPartials is a {@link Functor} that is used to sum observed duplicates from the tuple stream.
   * <p>
   *
   * @see SumByLocally
   */
  public static class SumPartials implements Functor
    {
    private final Fields declaredFields;
    private final Type sumType;
    private final CoercibleType canonical;

    /** Constructor SumPartials creates a new SumPartials instance. */
    public SumPartials( Fields declaredFields )
      {
      this.declaredFields = declaredFields;

      if( !declaredFields.hasTypes() )
        throw new IllegalArgumentException( "result type must be declared " );

      this.sumType = declaredFields.getType( 0 );

      if( declaredFields.size() != 1 )
        throw new IllegalArgumentException( "declared fields may only have one field, got: " + declaredFields );

      this.canonical = Coercions.coercibleTypeFor( this.sumType );
      }

    public SumPartials( Fields declaredFields, Class sumType )
      {
      this.declaredFields = declaredFields;
      this.sumType = sumType;

      if( declaredFields.size() != 1 )
        throw new IllegalArgumentException( "declared fields may only have one field, got: " + declaredFields );

      this.canonical = Coercions.coercibleTypeFor( this.sumType );
      }

    @Override
    public Fields getDeclaredFields()
      {
      return declaredFields;
      }

    @Override
    public Tuple aggregate( FlowProcess flowProcess, TupleEntry args, Tuple context )
      {
      if( context == null )
        return args.getTupleCopy();
      else if( args.getObject( 0 ) == null )
        return context;

      context.set( 0, context.getDouble( 0 ) + args.getDouble( 0 ) );

      return context;
      }

    @Override
    public Tuple complete( FlowProcess flowProcess, Tuple context )
      {
      context.set( 0, canonical.canonical( context.getObject( 0 ) ) );

      return context;
      }
    }

  /**
   * Constructor SumByLocally creates a new SumByLocally instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param valueField of type Fields
   * @param sumField   of type Fields
   */
  @ConstructorProperties({"valueField", "sumField"})
  public SumByLocally( Fields valueField, Fields sumField )
    {
    super( valueField, new SumPartials( sumField ) );
    }

  //////////////

  /**
   * Constructor SumByLocally creates a new SumByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "sumField"})
  public SumByLocally( Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField )
    {
    this( null, pipe, groupingFields, valueField, sumField, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor SumByLocally creates a new SumByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "sumField", "threshold"})
  public SumByLocally( Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, int threshold )
    {
    this( null, pipe, groupingFields, valueField, sumField, threshold );
    }

  /**
   * Constructor SumByLocally creates a new SumByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "sumField"})
  public SumByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField )
    {
    this( name, pipe, groupingFields, valueField, sumField, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor SumByLocally creates a new SumByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "sumField", "threshold"})
  public SumByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, int threshold )
    {
    super( name, pipe, groupingFields, valueField, new SumPartials( sumField ), threshold );
    }

///////////

  /**
   * Constructor SumByLocally creates a new SumByLocally instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param valueField of type Fields
   * @param sumField   of type Fields
   * @param sumType    of type Class
   */
  @ConstructorProperties({"valueField", "sumField", "sumType"})
  public SumByLocally( Fields valueField, Fields sumField, Class sumType )
    {
    super( valueField, new SumPartials( sumField, sumType ) );
    }

//////////////

  /**
   * Constructor SumByLocally creates a new SumByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "sumField", "sumType"})
  public SumByLocally( Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( null, pipe, groupingFields, valueField, sumField, sumType, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor SumByLocally creates a new SumByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "sumField", "sumType", "threshold"})
  public SumByLocally( Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    this( null, pipe, groupingFields, valueField, sumField, sumType, threshold );
    }

  /**
   * Constructor SumByLocally creates a new SumByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "sumField", "sumType"})
  public SumByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( name, pipe, groupingFields, valueField, sumField, sumType, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor SumByLocally creates a new SumByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "sumField", "sumType", "threshold"})
  public SumByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    super( name, pipe, groupingFields, valueField, new SumPartials( sumField, sumType ), threshold );
    }
  }
