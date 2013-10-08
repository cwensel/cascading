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
import cascading.operation.aggregator.MaxValue;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class MaxBy is used to find the maximum value in a grouping.
 * <p/>
 * Typically finding the max value of a field in a tuple stream relies on a {@link cascading.pipe.GroupBy} and a
 * {@link cascading.operation.aggregator.MaxValue} {@link cascading.operation.Aggregator} operation.
 * <p/>
 * This SubAssembly also uses the {@link cascading.pipe.assembly.MaxBy.MaxPartials} {@link cascading.pipe.assembly.AggregateBy.Functor}
 * to track the maximum value before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying MaxPartials functions how many unique key sums to accumulate
 * in the LRU cache, before emitting the least recently used entry.
 * <p/>
 * By default, either the value of {@link #AGGREGATE_BY_THRESHOLD} System property or {@link AggregateBy#DEFAULT_THRESHOLD}
 * will be used.
 *
 * @see AggregateBy
 */
public class MaxBy extends AggregateBy
  {
  /** DEFAULT_THRESHOLD */
  public static final int DEFAULT_THRESHOLD = 10000;

  public static class MaxPartials implements Functor
    {
    private final Fields declaredFields;

    public MaxPartials( Fields declaredFields )
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

      if( lhs.compareTo( rhs ) < 0 )
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
   * Constructor MaxBy creates a new MaxBy instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param valueField of type Fields
   * @param maxField   of type Fields
   */
  @ConstructorProperties({"valueField", "maxField"})
  public MaxBy( Fields valueField, Fields maxField )
    {
    super( valueField, new MaxPartials( maxField ), new MaxValue( maxField ) );
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
    this( null, pipe, groupingFields, valueField, maxField, USE_DEFAULT_THRESHOLD );
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
    this( name, pipe, groupingFields, valueField, maxField, USE_DEFAULT_THRESHOLD );
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
    this( null, pipes, groupingFields, valueField, maxField, USE_DEFAULT_THRESHOLD );
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
    this( name, pipes, groupingFields, valueField, maxField, USE_DEFAULT_THRESHOLD );
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
    super( name, pipes, groupingFields, valueField, new MaxPartials( maxField ), new MaxValue( maxField ), threshold );
    }
  }
