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

import cascading.flow.FlowProcess;
import cascading.operation.aggregator.First;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class FirstBy is used to return the first encountered Tuple in a tuple stream grouping.
 * <p/>
 * Typically finding the first Tuple in a tuple stream grouping relies on a {@link cascading.pipe.GroupBy} and a
 * {@link cascading.operation.aggregator.First} {@link cascading.operation.Aggregator} operation.
 * <p/>
 * If the {@code firstFields} argument has custom {@link java.util.Comparator} instances, they will be used
 * as the GroupBy {@code sortFields}.
 * <p/>
 * This SubAssembly also uses the {@link cascading.pipe.assembly.FirstBy.FirstPartials}
 * {@link cascading.pipe.assembly.AggregateBy.Functor}
 * to collect field values before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying FirstPartials functions how many unique key counts to accumulate
 * in the LRU cache, before emitting the least recently used entry.
 * <p/>
 * By default, either the value of {@link #AGGREGATE_BY_THRESHOLD} System property or {@link AggregateBy#DEFAULT_THRESHOLD}
 * will be used.
 *
 * @see AggregateBy
 */
public class FirstBy extends AggregateBy
  {
  /**
   * Class CountPartials is a {@link cascading.pipe.assembly.AggregateBy.Functor} that is used to count observed duplicates from the tuple stream.
   * <p/>
   * Use this class typically in tandem with a {@link cascading.operation.aggregator.Sum}
   * {@link cascading.operation.Aggregator} in order to improve counting performance by removing as many values
   * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
   *
   * @see cascading.pipe.assembly.FirstBy
   */
  public static class FirstPartials implements Functor
    {
    private final Fields declaredFields;

    /**
     * Constructor FirstPartials creates a new FirstPartials instance.
     *
     * @param declaredFields of type Fields
     */
    public FirstPartials( Fields declaredFields )
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
      if( context == null || args.getFields().compare( context, args.getTuple() ) > 0 )
        return args.getTupleCopy();

      return context;
      }

    @Override
    public Tuple complete( FlowProcess flowProcess, Tuple context )
      {
      return context;
      }
    }

  /**
   * Constructor FirstBy creates a new FirstBy instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param firstFields of type Fields
   */
  @ConstructorProperties({"firstFields"})
  public FirstBy( Fields firstFields )
    {
    super( firstFields, new FirstPartials( firstFields ), new First( firstFields ) );
    }

  /**
   * Constructor FirstBy creates a new FirstBy instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param firstFields of type Fields
   */
  @ConstructorProperties({"argumentFields", "firstFields"})
  public FirstBy( Fields argumentFields, Fields firstFields )
    {
    super( argumentFields, new FirstPartials( argumentFields ), new First( firstFields ) );
    }

  ///////

  /**
   * Constructor FirstBy creates a new FirstBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param firstFields    of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields", "firstFields"})
  public FirstBy( Pipe pipe, Fields groupingFields, Fields firstFields )
    {
    this( null, pipe, groupingFields, firstFields );
    }

  /**
   * Constructor FirstBy creates a new FirstBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param firstFields    fo type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "firstFields", "threshold"})
  public FirstBy( Pipe pipe, Fields groupingFields, Fields firstFields, int threshold )
    {
    this( null, pipe, groupingFields, firstFields, threshold );
    }

  /**
   * Constructor FirstBy creates a new FirstBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param firstFields    of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "firstFields"})
  public FirstBy( String name, Pipe pipe, Fields groupingFields, Fields firstFields )
    {
    this( name, pipe, groupingFields, firstFields, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor FirstBy creates a new FirstBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param firstFields    of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "firstFields", "threshold"})
  public FirstBy( String name, Pipe pipe, Fields groupingFields, Fields firstFields, int threshold )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, firstFields, threshold );
    }

  /**
   * Constructor FirstBy creates a new FirstBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param firstFields    of type Fields
   */
  @ConstructorProperties({"pipes", "groupingFields", "firstFields"})
  public FirstBy( Pipe[] pipes, Fields groupingFields, Fields firstFields )
    {
    this( null, pipes, groupingFields, firstFields, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor FirstBy creates a new FirstBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param firstFields    of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipes", "groupingFields", "firstFields", "threshold"})
  public FirstBy( Pipe[] pipes, Fields groupingFields, Fields firstFields, int threshold )
    {
    this( null, pipes, groupingFields, firstFields, threshold );
    }

  /**
   * Constructor FirstBy creates a new FirstBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param firstFields    of type Fields
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "firstFields"})
  public FirstBy( String name, Pipe[] pipes, Fields groupingFields, Fields firstFields )
    {
    this( name, pipes, groupingFields, firstFields, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor FirstBy creates a new FirstBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param firstFields    of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "firstFields", "threshold"})
  public FirstBy( String name, Pipe[] pipes, Fields groupingFields, Fields firstFields, int threshold )
    {
    super( name, pipes, groupingFields, firstFields, new FirstPartials( firstFields ), new First( firstFields ), threshold );
    }
  }
