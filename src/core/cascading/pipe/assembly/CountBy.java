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
import cascading.operation.aggregator.Sum;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class CountBy is used to count duplicates in a tuple stream.
 * <p/>
 * Typically finding the count of a field in a tuple stream relies on a {@link cascading.pipe.GroupBy} and a {@link cascading.operation.aggregator.Count}
 * {@link cascading.operation.Aggregator} operation.
 * <p/>
 * This SubAssembly also uses the {@link CountBy.CountPartials} {@link AggregateBy.Functor}
 * to count field values before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying CountPartials functions how many unique key counts to accumulate
 * in the LRU cache, before emitting the least recently used entry.
 *
 * @see AggregateBy
 */
public class CountBy extends AggregateBy
  {
  /**
   * Class CountPartials is a {@link AggregateBy.Functor} that is used to count observed duplicates from the tuple stream.
   * <p/>
   * Use this class typically in tandem with a {@link cascading.operation.aggregator.Sum}
   * {@link cascading.operation.Aggregator} in order to improve counting performance by removing as many values
   * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
   *
   * @see CountBy
   */
  public static class CountPartials implements Functor
    {
    private Fields declaredFields;

    /**
     * Constructor CountPartials creates a new CountPartials instance.
     *
     * @param declaredFields of type Fields
     */
    public CountPartials( Fields declaredFields )
      {
      this.declaredFields = declaredFields;

      if( !declaredFields.isDeclarator() || declaredFields.size() != 1 )
        throw new IllegalArgumentException( "declaredFields should declare only one field name" );
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
        context = new Tuple( 1L );
      else
        context.set( 0, context.getLong( 0 ) + 1L );

      return context;
      }

    @Override
    public Tuple complete( FlowProcess flowProcess, Tuple context )
      {
      return context;
      }
    }

  /**
   * Constructor CountBy creates a new CountBy instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param countField of type Fields
   */
  @ConstructorProperties({"countField"})
  public CountBy( Fields countField )
    {
    super( Fields.ALL, new CountPartials( countField ), new Sum( countField, Long.TYPE ) );
    }

  ///////

  /**
   * Constructor CountBy creates a new CountBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields", "countField"})
  public CountBy( Pipe pipe, Fields groupingFields, Fields countField )
    {
    this( null, pipe, groupingFields, countField );
    }

  /**
   * Constructor CountBy creates a new CountBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     fo type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "countField", "threshold"})
  public CountBy( Pipe pipe, Fields groupingFields, Fields countField, int threshold )
    {
    this( null, pipe, groupingFields, countField, threshold );
    }

  /**
   * Constructor CountBy creates a new CountBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "countField"})
  public CountBy( String name, Pipe pipe, Fields groupingFields, Fields countField )
    {
    this( name, pipe, groupingFields, countField, 10000 );
    }

  /**
   * Constructor CountBy creates a new CountBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "countField", "threshold"})
  public CountBy( String name, Pipe pipe, Fields groupingFields, Fields countField, int threshold )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, countField, threshold );
    }

  /**
   * Constructor CountBy creates a new CountBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   */
  @ConstructorProperties({"pipes", "groupingFields", "countField"})
  public CountBy( Pipe[] pipes, Fields groupingFields, Fields countField )
    {
    this( null, pipes, groupingFields, countField, 10000 );
    }

  /**
   * Constructor CountBy creates a new CountBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipes", "groupingFields", "countField", "threshold"})
  public CountBy( Pipe[] pipes, Fields groupingFields, Fields countField, int threshold )
    {
    this( null, pipes, groupingFields, countField, threshold );
    }

  /**
   * Constructor CountBy creates a new CountBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "countField"})
  public CountBy( String name, Pipe[] pipes, Fields groupingFields, Fields countField )
    {
    this( name, pipes, groupingFields, countField, 10000 );
    }

  /**
   * Constructor CountBy creates a new CountBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "countField", "threshold"})
  public CountBy( String name, Pipe[] pipes, Fields groupingFields, Fields countField, int threshold )
    {
    super( name, pipes, groupingFields, groupingFields, new CountPartials( countField ), new Sum( countField, Long.TYPE ), threshold );
    }
  }
