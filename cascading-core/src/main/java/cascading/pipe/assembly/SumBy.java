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

package cascading.pipe.assembly;

import java.beans.ConstructorProperties;
import java.lang.reflect.Type;

import cascading.flow.FlowProcess;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;

/**
 * Class SumBy is used to sum values associated with duplicate keys in a tuple stream.
 * <p/>
 * Typically finding the sum of field in a tuple stream relies on a {@link cascading.pipe.GroupBy} and a {@link cascading.operation.aggregator.Sum}
 * {@link cascading.operation.Aggregator} operation.
 * <p/>
 * If all the values to be summed are all {@code null}, the result value is a function of how null is coerced by the
 * given {@code sumType}. If a primitive type, {@code 0} will be returned. Otherwise {@code null}.
 * <p/>
 * This SubAssembly also uses the {@link SumBy.SumPartials} {@link AggregateBy.Functor}
 * to sum field values before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying SumPartials functions how many unique key sums to accumulate
 * in the LRU cache, before emitting the least recently used entry.
 *
 * @see AggregateBy
 */
public class SumBy extends AggregateBy
  {
  /** DEFAULT_THRESHOLD */
  public static final int DEFAULT_THRESHOLD = 10000;

  /**
   * Class SumPartials is a {@link AggregateBy.Functor} that is used to sum observed duplicates from the tuple stream.
   * <p/>
   * Use this class typically in tandem with a {@link cascading.operation.aggregator.Sum}
   * {@link cascading.operation.Aggregator} in order to improve counting performance by removing as many values
   * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
   *
   * @see SumBy
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
   * Constructor SumBy creates a new SumBy instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param valueField of type Fields
   * @param sumField   of type Fields
   */
  @ConstructorProperties({"valueField", "sumField"})
  public SumBy( Fields valueField, Fields sumField )
    {
    super( valueField, new SumPartials( sumField ), new Sum( sumField ) );
    }

  //////////////

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "sumField"})
  public SumBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField )
    {
    this( null, pipe, groupingFields, valueField, sumField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "sumField", "threshold"})
  public SumBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, int threshold )
    {
    this( null, pipe, groupingFields, valueField, sumField, threshold );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "sumField"})
  public SumBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField )
    {
    this( name, pipe, groupingFields, valueField, sumField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "sumField", "threshold"})
  public SumBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, int threshold )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, valueField, sumField, threshold );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "sumField"})
  public SumBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields sumField )
    {
    this( null, pipes, groupingFields, valueField, sumField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "sumField", "threshold"})
  public SumBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields sumField, int threshold )
    {
    this( null, pipes, groupingFields, valueField, sumField, threshold );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "sumField"})
  public SumBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields sumField )
    {
    this( name, pipes, groupingFields, valueField, sumField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "sumField", "threshold"})
  public SumBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields sumField, int threshold )
    {
    super( name, pipes, groupingFields, valueField, new SumPartials( sumField ), new Sum( sumField ), threshold );
    }

///////////

  /**
   * Constructor SumBy creates a new SumBy instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param valueField of type Fields
   * @param sumField   of type Fields
   * @param sumType    of type Class
   */
  @ConstructorProperties({"valueField", "sumField", "sumType"})
  public SumBy( Fields valueField, Fields sumField, Class sumType )
    {
    super( valueField, new SumPartials( sumField, sumType ), new Sum( sumField, sumType ) );
    }

//////////////

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "sumField", "sumType"})
  public SumBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( null, pipe, groupingFields, valueField, sumField, sumType, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "sumField", "sumType", "threshold"})
  public SumBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    this( null, pipe, groupingFields, valueField, sumField, sumType, threshold );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "sumField", "sumType"})
  public SumBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( name, pipe, groupingFields, valueField, sumField, sumType, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
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
  public SumBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, valueField, sumField, sumType, threshold );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "sumField", "sumType"})
  public SumBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( null, pipes, groupingFields, valueField, sumField, sumType, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "sumField", "sumType", "threshold"})
  public SumBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    this( null, pipes, groupingFields, valueField, sumField, sumType, threshold );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "sumField", "sumType"})
  public SumBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( name, pipes, groupingFields, valueField, sumField, sumType, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor SumBy creates a new SumBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "sumField", "sumType", "threshold"})
  public SumBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    super( name, pipes, groupingFields, valueField, new SumPartials( sumField, sumType ), new Sum( sumField, sumType ), threshold );
    }
  }
