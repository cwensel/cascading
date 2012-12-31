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
import cascading.operation.aggregator.Min;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;

/**
 * Class MinBy is used to find the minimum value in a grouping.
 * <p/>
 * Typically finding the min value of a field in a tuple stream relies on a {@link cascading.pipe.GroupBy} and a {@link cascading.operation.aggregator.Min}
 * {@link cascading.operation.Aggregator} operation.
 * <p/>
 * This SubAssembly also uses the {@link cascading.pipe.assembly.MinBy.MinPartials} {@link cascading.pipe.assembly.AggregateBy.Functor}
 * to track the minimum value before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying MinPartials functions how many unique key sums to accumulate
 * in the LRU cache, before emitting the least recently used entry.
 *
 * @see cascading.pipe.assembly.AggregateBy
 */
public class MinBy extends AggregateBy
  {
  /** DEFAULT_THRESHOLD */
  public static final int DEFAULT_THRESHOLD = 10000;

  /**
   * Class MinPartials is a {@link cascading.pipe.assembly.AggregateBy.Functor} that is used to sum observed duplicates from the tuple stream.
   * <p/>
   * Use this class typically in tandem with a {@link cascading.operation.aggregator.Sum}
   * {@link cascading.operation.Aggregator} in order to improve counting performance by removing as many values
   * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
   *
   * @see cascading.pipe.assembly.MinBy
   */
  public static class MinPartials implements Functor
    {
    private final Fields declaredFields;
    private final Type type;
    private final CoercibleType canonical;

    /** Constructor MinPartials creates a new MinPartials instance. */
    public MinPartials( Fields declaredFields )
      {
      this.declaredFields = declaredFields;

      if( !declaredFields.hasTypes() )
        throw new IllegalArgumentException( "result type must be declared " );

      this.type = declaredFields.getType( 0 );

      if( declaredFields.size() != 1 )
        throw new IllegalArgumentException( "declared fields may only have one field, got: " + declaredFields );

      this.canonical = Coercions.coercibleTypeFor( this.type );
      }

    public MinPartials( Fields declaredFields, Class type )
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

      context.set( 0, Math.min( context.getDouble( 0 ), args.getDouble( 0 ) ) );

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
   * Constructor MinBy creates a new MinBy instance. Use this constructor when used with a {@link cascading.pipe.assembly.AggregateBy}
   * instance.
   *
   * @param valueField of type Fields
   * @param minField   of type Fields
   */
  @ConstructorProperties({"valueField", "minField"})
  public MinBy( Fields valueField, Fields minField )
    {
    super( valueField, new MinPartials( minField ), new Min( minField ) );
    }

  //////////////

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "minField"})
  public MinBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields minField )
    {
    this( null, pipe, groupingFields, valueField, minField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "minField", "threshold"})
  public MinBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields minField, int threshold )
    {
    this( null, pipe, groupingFields, valueField, minField, threshold );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "minField"})
  public MinBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields minField )
    {
    this( name, pipe, groupingFields, valueField, minField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "minField", "threshold"})
  public MinBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields minField, int threshold )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, valueField, minField, threshold );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "minField"})
  public MinBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields minField )
    {
    this( null, pipes, groupingFields, valueField, minField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "minField", "threshold"})
  public MinBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields minField, int threshold )
    {
    this( null, pipes, groupingFields, valueField, minField, threshold );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "minField"})
  public MinBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields minField )
    {
    this( name, pipes, groupingFields, valueField, minField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "minField", "threshold"})
  public MinBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields minField, int threshold )
    {
    super( name, pipes, groupingFields, valueField, new MinPartials( minField ), new Sum( minField ), threshold );
    }

///////////

  /**
   * Constructor MinBy creates a new MinBy instance. Use this constructor when used with a {@link cascading.pipe.assembly.AggregateBy}
   * instance.
   *
   * @param valueField of type Fields
   * @param minField   of type Fields
   * @param minType    of type Class
   */
  @ConstructorProperties({"valueField", "minField", "minType"})
  public MinBy( Fields valueField, Fields minField, Class minType )
    {
    super( valueField, new MinPartials( minField, minType ), new Sum( minField, minType ) );
    }

//////////////

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param minType        of type Class
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "minField", "minType"})
  public MinBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields minField, Class minType )
    {
    this( null, pipe, groupingFields, valueField, minField, minType, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param minType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "minField", "minType", "threshold"})
  public MinBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields minField, Class minType, int threshold )
    {
    this( null, pipe, groupingFields, valueField, minField, minType, threshold );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param minType        of type Class
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "minField", "minType"})
  public MinBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields minField, Class minType )
    {
    this( name, pipe, groupingFields, valueField, minField, minType, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param minType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "minField", "minType", "threshold"})
  public MinBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields minField, Class minType, int threshold )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, valueField, minField, minType, threshold );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param minType        of type Class
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "minField", "minType"})
  public MinBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields minField, Class minType )
    {
    this( null, pipes, groupingFields, valueField, minField, minType, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param minType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "minField", "minType", "threshold"})
  public MinBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields minField, Class minType, int threshold )
    {
    this( null, pipes, groupingFields, valueField, minField, minType, threshold );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param minType        of type Class
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "minField", "minType"})
  public MinBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields minField, Class minType )
    {
    this( name, pipes, groupingFields, valueField, minField, minType, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MinBy creates a new MinBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param minType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "minField", "minType", "threshold"})
  public MinBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields minField, Class minType, int threshold )
    {
    super( name, pipes, groupingFields, valueField, new MinPartials( minField, minType ), new Min( minField, minType ), threshold );
    }
  }
