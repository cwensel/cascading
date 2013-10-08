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

import cascading.flow.FlowProcess;
import cascading.operation.aggregator.MinValue;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class MinBy is used to find the minimum value in a grouping.
 * <p/>
 * Typically finding the min value of a field in a tuple stream relies on a {@link cascading.pipe.GroupBy} and a
 * {@link cascading.operation.aggregator.MinValue} {@link cascading.operation.Aggregator} operation.
 * <p/>
 * This SubAssembly also uses the {@link cascading.pipe.assembly.MinBy.MinPartials} {@link cascading.pipe.assembly.AggregateBy.Functor}
 * to track the minimum value before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying MinPartials functions how many unique key sums to accumulate
 * in the LRU cache, before emitting the least recently used entry.
 * <p/>
 * By default, either the value of {@link #AGGREGATE_BY_THRESHOLD} System property or {@link AggregateBy#DEFAULT_THRESHOLD}
 * will be used.
 *
 * @see cascading.pipe.assembly.AggregateBy
 */
public class MinBy extends AggregateBy
  {
  /** DEFAULT_THRESHOLD */
  @Deprecated
  public static final int DEFAULT_THRESHOLD = 10000;

  public static class MinPartials implements Functor
    {
    private final Fields declaredFields;

    /** Constructor MinPartials creates a new MinPartials instance. */
    public MinPartials( Fields declaredFields )
      {
      this.declaredFields = declaredFields;

      if( declaredFields.size() != 1 )
        throw new IllegalArgumentException( "declared fields may only have one field, got: " + declaredFields );
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

      Comparable lhs = (Comparable) context.getObject( 0 );
      Comparable rhs = (Comparable) args.getObject( 0 );

      if( lhs.compareTo( rhs ) > 0 )
        context.set( 0, rhs );

      return context;
      }

    @Override
    public Tuple complete( FlowProcess flowProcess, Tuple context )
      {
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
    super( valueField, new MinPartials( minField ), new MinValue( minField ) );
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
    this( null, pipe, groupingFields, valueField, minField, 0 );
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
    this( name, pipe, groupingFields, valueField, minField, USE_DEFAULT_THRESHOLD );
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
    this( null, pipes, groupingFields, valueField, minField, USE_DEFAULT_THRESHOLD );
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
    this( name, pipes, groupingFields, valueField, minField, USE_DEFAULT_THRESHOLD );
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
    super( name, pipes, groupingFields, valueField, new MinPartials( minField ), new MinValue( minField ), threshold );
    }
  }
