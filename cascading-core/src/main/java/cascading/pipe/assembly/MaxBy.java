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

package cascading.pipe.assembly;

import java.beans.ConstructorProperties;
import java.lang.reflect.Type;

import cascading.flow.FlowProcess;
import cascading.operation.aggregator.Max;
import cascading.operation.aggregator.Min;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;

/**
 * Class MaxBy is used to find the maximum value in a grouping.
 * <p/>
 * Typically finding the max value of a field in a tuple stream relies on a {@link cascading.pipe.GroupBy} and a {@link cascading.operation.aggregator.Min}
 * {@link cascading.operation.Aggregator} operation.
 * <p/>
 * This SubAssembly also uses the {@link cascading.pipe.assembly.MaxBy.MaxPartials} {@link cascading.pipe.assembly.AggregateBy.Functor}
 * to track the minimum value before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying MaxPartials functions how many unique key sums to accumulate
 * in the LRU cache, before emitting the least recently used entry.
 *
 * @see AggregateBy
 */
public class MaxBy extends AggregateBy
  {
  /** DEFAULT_THRESHOLD */
  public static final int DEFAULT_THRESHOLD = 10000;

  /**
   * Class MaxPartials is a {@link cascading.pipe.assembly.AggregateBy.Functor} that is used to sum observed duplicates from the tuple stream.
   * <p/>
   * Use this class typically in tandem with a {@link cascading.operation.aggregator.Sum}
   * {@link cascading.operation.Aggregator} in order to improve counting performance by removing as many values
   * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
   *
   * @see cascading.pipe.assembly.MaxBy
   */
  public static class MaxPartials implements Functor
    {
    private final Fields declaredFields;
    private final Type type;
    private final CoercibleType canonical;

    /** Constructor MaxPartials creates a new MaxPartials instance. */
    public MaxPartials( Fields declaredFields )
      {
      this.declaredFields = declaredFields;

      if( !declaredFields.hasTypes() )
        throw new IllegalArgumentException( "result type must be declared " );

      this.type = declaredFields.getType( 0 );

      if( declaredFields.size() != 1 )
        throw new IllegalArgumentException( "declared fields may only have one field, got: " + declaredFields );

      this.canonical = Coercions.coercibleTypeFor( this.type );
      }

    public MaxPartials( Fields declaredFields, Class type )
      {
      this.declaredFields = declaredFields;
      this.type = type;

      if( declaredFields.size() != 1 )
        throw new IllegalArgumentException( "declared fields may only have one field, got: " + declaredFields );

      this.canonical = Coercions.coercibleTypeFor( this.type );
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

      context.set( 0, Math.max( context.getDouble( 0 ), args.getDouble( 0 ) ) );

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
   * Constructor MaxBy creates a new MaxBy instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param valueField of type Fields
   * @param maxField   of type Fields
   */
  @ConstructorProperties({"valueField", "maxField"})
  public MaxBy( Fields valueField, Fields maxField )
    {
    super( valueField, new MaxPartials( maxField ), new Min( maxField ) );
    }

  //////////////

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "maxField"})
  public MaxBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields maxField )
    {
    this( null, pipe, groupingFields, valueField, maxField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "maxField", "threshold"})
  public MaxBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields maxField, int threshold )
    {
    this( null, pipe, groupingFields, valueField, maxField, threshold );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "maxField"})
  public MaxBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields maxField )
    {
    this( name, pipe, groupingFields, valueField, maxField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "maxField", "threshold"})
  public MaxBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields maxField, int threshold )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, valueField, maxField, threshold );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "maxField"})
  public MaxBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields maxField )
    {
    this( null, pipes, groupingFields, valueField, maxField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "maxField", "threshold"})
  public MaxBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields maxField, int threshold )
    {
    this( null, pipes, groupingFields, valueField, maxField, threshold );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "maxField"})
  public MaxBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields maxField )
    {
    this( name, pipes, groupingFields, valueField, maxField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "maxField", "threshold"})
  public MaxBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields maxField, int threshold )
    {
    super( name, pipes, groupingFields, valueField, new MaxPartials( maxField ), new Sum( maxField ), threshold );
    }

///////////

  /**
   * Constructor MaxBy creates a new MaxBy instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param valueField of type Fields
   * @param maxField   of type Fields
   * @param maxType    of type Class
   */
  @ConstructorProperties({"valueField", "maxField", "maxType"})
  public MaxBy( Fields valueField, Fields maxField, Class maxType )
    {
    super( valueField, new MaxPartials( maxField, maxType ), new Sum( maxField, maxType ) );
    }

//////////////

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   * @param maxType        of type Class
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "maxField", "maxType"})
  public MaxBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields maxField, Class maxType )
    {
    this( null, pipe, groupingFields, valueField, maxField, maxType, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   * @param maxType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "maxField", "maxType", "threshold"})
  public MaxBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields maxField, Class maxType, int threshold )
    {
    this( null, pipe, groupingFields, valueField, maxField, maxType, threshold );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   * @param maxType        of type Class
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "maxField", "maxType"})
  public MaxBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields maxField, Class maxType )
    {
    this( name, pipe, groupingFields, valueField, maxField, maxType, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   * @param maxType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "maxField", "maxType", "threshold"})
  public MaxBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields maxField, Class maxType, int threshold )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, valueField, maxField, maxType, threshold );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   * @param maxType        of type Class
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "maxField", "maxType"})
  public MaxBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields maxField, Class maxType )
    {
    this( null, pipes, groupingFields, valueField, maxField, maxType, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   * @param maxType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "maxField", "maxType", "threshold"})
  public MaxBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields maxField, Class maxType, int threshold )
    {
    this( null, pipes, groupingFields, valueField, maxField, maxType, threshold );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   * @param maxType        of type Class
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "maxField", "maxType"})
  public MaxBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields maxField, Class maxType )
    {
    this( name, pipes, groupingFields, valueField, maxField, maxType, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MaxBy creates a new MaxBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param maxField       of type Fields
   * @param maxType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "maxField", "maxType", "threshold"})
  public MaxBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields maxField, Class maxType, int threshold )
    {
    super( name, pipes, groupingFields, valueField, new MaxPartials( maxField, maxType ), new Max( maxField, maxType ), threshold );
    }
  }
