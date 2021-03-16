/*
 * Copyright (c) 2016-2021 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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
import java.lang.reflect.Type;

import cascading.flow.FlowProcess;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;

/**
 * Class AverageByLocally is used to average values associated with duplicate keys in a tuple stream.
 * <p>
 * It is important to read about the {@link AggregateByLocally} base class and the subsequent limitation of this class
 * in relation to the {@link AverageBy} subassembly.
 * <p>
 * If the given {@code averageFields} has an associated type, this type will be used to coerce the resulting average value,
 * otherwise the result will be a {@link Double}.
 * <p>
 * If {@code include} is {@link Include#NO_NULLS}, {@code null} values will not be included in the average (converted to zero).
 * By default {@code null} values are included, {@link Include#ALL}.
 * <p>
 * The {@code threshold} value tells the underlying AveragePartials functions how many unique key sums and counts to accumulate
 * in the LRU cache, before emitting the least recently used entry. This accumulation happens map-side, and thus is
 * bounded by the size of your map task JVM and the typical size of each group key.
 * <p>
 * By default, either the value of {@link AggregateByLocallyProps#AGGREGATE_LOCALLY_BY_CAPACITY} System property
 * or {@link AggregateByLocallyProps#AGGREGATE_LOCALLY_BY_DEFAULT_CAPACITY} will be used.
 *
 * @see AggregateBy
 */
public class AverageByLocally extends AggregateByLocally
  {
  public enum Include
    {
      ALL,
      NO_NULLS
    }

  /**
   * Class AveragePartials is a {@link Functor} that is used to count and sum observed duplicates from the tuple stream.
   *
   * @see AverageByLocally
   */
  public static class AveragePartials implements Functor
    {
    private final Fields declaredFields;
    private final Include include;
    private final Type type;
    private final CoercibleType canonical;

    /**
     * Constructor AveragePartials creates a new AveragePartials instance.
     *
     * @param declaredFields of type Fields
     */
    public AveragePartials( Fields declaredFields )
      {
      this( declaredFields, Include.ALL );
      }

    public AveragePartials( Fields declaredFields, Include include )
      {
      this.declaredFields = makeFieldDeclaration( declaredFields );
      this.include = include;

      if( this.declaredFields.hasTypes() )
        this.type = this.declaredFields.getType( 0 );
      else
        this.type = Double.class;

      this.canonical = Coercions.coercibleTypeFor( this.type );
      }

    private static Fields makeFieldDeclaration( Fields fieldDeclaration )
      {
      if( fieldDeclaration.hasTypes() )
        return fieldDeclaration;

      return fieldDeclaration.applyTypes( Double.class );
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
        context = Tuple.size( 2 );

      if( include == Include.NO_NULLS && args.getObject( 0 ) == null )
        return context;

      context.set( 0, context.getDouble( 0 ) + args.getDouble( 0 ) );
      context.set( 1, context.getLong( 1 ) + 1 );

      return context;
      }

    @Override
    public Tuple complete( FlowProcess flowProcess, Tuple context )
      {
      long count = context.getLong( 1 );

      if( count == 0 )
        return new Tuple( canonical.canonical( null ) );

      double sum = context.getDouble( 0 );

      return new Tuple( canonical.canonical( sum / count ) );
      }
    }

  /**
   * Constructor AverageByLocally creates a new AverageByLocally instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param valueField   of type Fields
   * @param averageField of type Fields
   */
  @ConstructorProperties({"valueField", "averageField"})
  public AverageByLocally( Fields valueField, Fields averageField )
    {
    super( valueField, new AveragePartials( averageField ) );
    }

  /**
   * Constructor AverageByLocally creates a new AverageByLocally instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param valueField   of type Fields
   * @param averageField of type Fields
   * @param include      of type boolean
   */
  @ConstructorProperties({"valueField", "averageField", "include"})
  public AverageByLocally( Fields valueField, Fields averageField, Include include )
    {
    super( valueField, new AveragePartials( averageField, include ) );
    }

  //////////////

  /**
   * Constructor AverageByLocally creates a new AverageByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "averageField"})
  public AverageByLocally( Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField )
    {
    this( null, pipe, groupingFields, valueField, averageField, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor AverageByLocally creates a new AverageByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "averageField", "threshold"})
  public AverageByLocally( Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField, int threshold )
    {
    this( null, pipe, groupingFields, valueField, averageField, threshold );
    }

  /**
   * Constructor AverageByLocally creates a new AverageByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "averageField"})
  public AverageByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField )
    {
    this( name, pipe, groupingFields, valueField, averageField, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor AverageByLocally creates a new AverageByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "averageField", "threshold"})
  public AverageByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField, int threshold )
    {
    super( name, pipe, groupingFields, valueField, new AveragePartials( averageField ), threshold );
    }

  /**
   * Constructor AverageByLocally creates a new AverageByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param include        of type boolean
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "averageField", "include"})
  public AverageByLocally( Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField, Include include )
    {
    this( null, pipe, groupingFields, valueField, averageField, include, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor AverageByLocally creates a new AverageByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param include        of type boolean
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "averageField", "include", "threshold"})
  public AverageByLocally( Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField, Include include, int threshold )
    {
    this( null, pipe, groupingFields, valueField, averageField, include, threshold );
    }

  /**
   * Constructor AverageByLocally creates a new AverageByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param include        of type boolean
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "averageField", "include"})
  public AverageByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField, Include include )
    {
    this( name, pipe, groupingFields, valueField, averageField, include, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor AverageByLocally creates a new AverageByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param include        of type boolean
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "averageField", "include", "threshold"})
  public AverageByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField, Include include, int threshold )
    {
    super( name, pipe, groupingFields, valueField, new AveragePartials( averageField, include ), threshold );
    }
  }
