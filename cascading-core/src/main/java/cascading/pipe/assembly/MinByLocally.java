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

import cascading.flow.FlowProcess;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class MinByLocally is used to find the minimum value in a grouping.
 * <p>
 * It is important to read about the {@link AggregateByLocally} base class and the subsequent limitation of this class
 * in relation to the {@link MinBy} subassembly.
 * <p>
 * This SubAssembly also uses the {@link MinByLocally.MinPartials} {@link Functor}
 * to track the minimum value before the GroupBy operator to reduce IO over the network.
 * <p>
 * The {@code threshold} value tells the underlying MinPartials functions how many unique key sums to accumulate
 * in the LRU cache, before emitting the least recently used entry. This accumulation happens map-side, and thus is
 * bounded by the size of your map task JVM and the typical size of each group key.
 * <p>
 * By default, either the value of {@link AggregateByProps#AGGREGATE_BY_CAPACITY} System property
 * or {@link AggregateByProps#AGGREGATE_BY_DEFAULT_CAPACITY} will be used.
 *
 * @see AggregateBy
 */
public class MinByLocally extends AggregateByLocally
  {
  public static class MinPartials implements Functor
    {
    private final Fields declaredFields;

    /**
     * Constructor MinPartials creates a new MinPartials instance.
     */
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

      if( ( lhs == null ) || ( lhs.compareTo( rhs ) > 0 ) )
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
   * Constructor MinByLocally creates a new MinByLocally instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param valueField of type Fields
   * @param minField   of type Fields
   */
  @ConstructorProperties({"valueField", "minField"})
  public MinByLocally( Fields valueField, Fields minField )
    {
    super( valueField, new MinPartials( minField ) );
    }

  //////////////

  /**
   * Constructor MinByLocally creates a new MinByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "minField"})
  public MinByLocally( Pipe pipe, Fields groupingFields, Fields valueField, Fields minField )
    {
    this( null, pipe, groupingFields, valueField, minField, 0 );
    }

  /**
   * Constructor MinByLocally creates a new MinByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "minField", "threshold"})
  public MinByLocally( Pipe pipe, Fields groupingFields, Fields valueField, Fields minField, int threshold )
    {
    this( null, pipe, groupingFields, valueField, minField, threshold );
    }

  /**
   * Constructor MinByLocally creates a new MinByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "minField"})
  public MinByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields minField )
    {
    this( name, pipe, groupingFields, valueField, minField, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor MinByLocally creates a new MinByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param minField       of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "minField", "threshold"})
  public MinByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields minField, int threshold )
    {
    super( name, pipe, groupingFields, valueField, new MinPartials( minField ), threshold );
    }
  }
