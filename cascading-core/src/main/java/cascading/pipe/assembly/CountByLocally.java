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

import cascading.flow.FlowProcess;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;

/**
 * Class CountByLocally is used to count duplicates in a tuple stream, where "duplicates" means all tuples with the same
 * values for the groupingFields fields. The resulting count is output as a long value in the specified countField.
 * <p>
 * It is important to read about the {@link AggregateByLocally} base class and the subsequent limitation of this class
 * in relation to the {@link CountBy} subassembly.
 * <p>
 * If {@code include} is {@link Include#NO_NULLS}, argument tuples with all null values will be ignored.
 * <p>
 * The values in the argument Tuple are normally all the remaining fields not used for grouping, but this can be
 * narrowed using the valueFields parameter. When counting the occurrence of a single field (when {@code valueFields}
 * is set on the constructor), this is the same behavior as {@code select count(foo) ...} in SQL. If {@code include} is
 * {@link Include#ONLY_NULLS} then only argument tuples with all null values will be counted.
 * <p>
 * The {@code threshold} value tells the underlying CountPartials functions how many unique key sums to accumulate
 * in the LRU cache, before emitting the least recently used entry. This accumulation is
 * bounded by the size of your map task JVM and the typical size of each group key.
 * <p>
 * By default, either the value of {@link AggregateByLocallyProps#AGGREGATE_LOCALLY_BY_CAPACITY} System property
 * or {@link AggregateByLocallyProps#AGGREGATE_LOCALLY_BY_DEFAULT_CAPACITY} will be used.
 *
 * @see AggregateByLocally
 * @see AggregateBy
 */
public class CountByLocally extends AggregateByLocally
  {
  public enum Include
    {
      ALL,
      NO_NULLS,
      ONLY_NULLS
    }

  /**
   * Class CountPartials is a {@link Functor} that is used to count observed duplicates from the tuple stream.
   * <p>
   * Use this class typically in tandem with a {@link Sum}
   * {@link cascading.operation.Aggregator} in order to improve counting performance by removing as many values
   * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
   *
   * @see CountByLocally
   */
  public static class CountPartials implements Functor
    {
    private final Fields declaredFields;
    private final Include include;

    /**
     * Constructor CountPartials creates a new CountPartials instance.
     *
     * @param declaredFields of type Fields
     */
    public CountPartials( Fields declaredFields )
      {
      this( declaredFields, Include.ALL );
      }

    public CountPartials( Fields declaredFields, Include include )
      {
      this.declaredFields = declaredFields;

      if( include == null )
        include = Include.ALL;

      this.include = include;

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
        context = new Tuple( 0L );

      switch( include )
        {
        case ALL:
          break;

        case NO_NULLS:
          if( Tuples.frequency( args, null ) == args.size() )
            return context;

          break;

        case ONLY_NULLS:
          if( Tuples.frequency( args, null ) != args.size() )
            return context;

          break;
        }

      context.set( 0, context.getLong( 0 ) + 1L );

      return context;
      }

    @Override
    public Tuple complete( FlowProcess flowProcess, Tuple context )
      {
      return context;
      }
    }

  //// AggregateByLocally param constructors

  /**
   * Constructor CountByLocally creates a new CountByLocally instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param countField of type Fields
   */
  @ConstructorProperties({"countField"})
  public CountByLocally( Fields countField )
    {
    super( Fields.ALL, new CountPartials( countField.applyTypes( Long.TYPE ) ) );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param countField of type Fields
   * @param include    of type Include
   */
  @ConstructorProperties({"countField", "include"})
  public CountByLocally( Fields countField, Include include )
    {
    super( Fields.ALL, new CountPartials( countField.applyTypes( Long.TYPE ), include ) );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param countField of type Fields
   */
  @ConstructorProperties({"valueFields", "countField"})
  public CountByLocally( Fields valueFields, Fields countField )
    {
    super( valueFields, new CountPartials( countField.applyTypes( Long.TYPE ) ) );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance. Use this constructor when used with a {@link AggregateBy}
   * instance.
   *
   * @param countField of type Fields
   */
  @ConstructorProperties({"valueFields", "countField", "include"})
  public CountByLocally( Fields valueFields, Fields countField, Include include )
    {
    super( valueFields, new CountPartials( countField.applyTypes( Long.TYPE ), include ) );
    }

  ///////

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields", "countField"})
  public CountByLocally( Pipe pipe, Fields groupingFields, Fields countField )
    {
    this( null, pipe, groupingFields, countField );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     fo type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "countField", "threshold"})
  public CountByLocally( Pipe pipe, Fields groupingFields, Fields countField, int threshold )
    {
    this( null, pipe, groupingFields, countField, threshold );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "countField"})
  public CountByLocally( String name, Pipe pipe, Fields groupingFields, Fields countField )
    {
    this( name, pipe, groupingFields, countField, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "countField", "threshold"})
  public CountByLocally( String name, Pipe pipe, Fields groupingFields, Fields countField, int threshold )
    {
    super( name, pipe, groupingFields, groupingFields, new CountPartials( countField.applyTypes( Long.TYPE ) ), threshold );
    }

  ///////

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   * @param include        of type Include
   */
  @ConstructorProperties({"pipe", "groupingFields", "countField", "include"})
  public CountByLocally( Pipe pipe, Fields groupingFields, Fields countField, Include include )
    {
    this( null, pipe, groupingFields, countField, include );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     fo type Fields
   * @param include        of type Include
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "countField", "include", "threshold"})
  public CountByLocally( Pipe pipe, Fields groupingFields, Fields countField, Include include, int threshold )
    {
    this( null, pipe, groupingFields, countField, include, threshold );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   * @param include        of type Include
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "countField", "include"})
  public CountByLocally( String name, Pipe pipe, Fields groupingFields, Fields countField, Include include )
    {
    this( name, pipe, groupingFields, countField, include, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   * @param include        of type Include
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "countField", "include", "threshold"})
  public CountByLocally( String name, Pipe pipe, Fields groupingFields, Fields countField, Include include, int threshold )
    {
    super( name, pipe, groupingFields, groupingFields, new CountPartials( countField.applyTypes( Long.TYPE ), include ), threshold );
    }

////////////

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueFields    of type Fields
   * @param countField     of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueFields", "countField"})
  public CountByLocally( Pipe pipe, Fields groupingFields, Fields valueFields, Fields countField )
    {
    this( null, pipe, groupingFields, valueFields, countField, Include.ALL );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueFields    of type Fields
   * @param countField     fo type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueFields", "countField", "threshold"})
  public CountByLocally( Pipe pipe, Fields groupingFields, Fields valueFields, Fields countField, int threshold )
    {
    this( null, pipe, groupingFields, valueFields, countField, threshold );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueFields    of type Fields
   * @param countField     of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueFields", "countField"})
  public CountByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueFields, Fields countField )
    {
    this( name, pipe, groupingFields, valueFields, countField, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueFields    of type Fields
   * @param countField     of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueFields", "countField", "threshold"})
  public CountByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueFields, Fields countField, int threshold )
    {
    super( name, pipe, groupingFields, valueFields, new CountPartials( countField.applyTypes( Long.TYPE ) ), threshold );
    }

////////////

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueFields    of type Fields
   * @param countField     of type Fields
   * @param include        of type Include
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueFields", "countField", "include"})
  public CountByLocally( Pipe pipe, Fields groupingFields, Fields valueFields, Fields countField, Include include )
    {
    this( null, pipe, groupingFields, valueFields, countField, include );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueFields    of type Fields
   * @param countField     fo type Fields
   * @param include        of type Include
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueFields", "countField", "include", "threshold"})
  public CountByLocally( Pipe pipe, Fields groupingFields, Fields valueFields, Fields countField, Include include, int threshold )
    {
    this( null, pipe, groupingFields, valueFields, countField, include, threshold );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param valueFields    of type Fields
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   * @param include        of type Include
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueFields", "countField", "include"})
  public CountByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueFields, Fields countField, Include include )
    {
    this( name, pipe, groupingFields, valueFields, countField, include, USE_DEFAULT_THRESHOLD );
    }

  /**
   * Constructor CountByLocally creates a new CountByLocally instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueFields    of type Fields
   * @param countField     of type Fields
   * @param include        of type Include
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueFields", "countField", "include", "threshold"})
  public CountByLocally( String name, Pipe pipe, Fields groupingFields, Fields valueFields, Fields countField, Include include, int threshold )
    {
    super( name, pipe, groupingFields, valueFields, new CountPartials( countField.applyTypes( Long.TYPE ), include ), threshold );
    }
  }
