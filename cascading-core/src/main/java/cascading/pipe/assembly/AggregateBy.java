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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.management.annotation.Property;
import cascading.management.annotation.PropertyConfigured;
import cascading.management.annotation.PropertyDescription;
import cascading.management.annotation.Visibility;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.cache.BaseCacheFactory;

/**
 * Class AggregateBy is a {@link SubAssembly} that serves two roles for handling aggregate operations.
 * <p>
 * The first role is as a base class for composable aggregate operations that have a MapReduce Map side optimization for the
 * Reduce side aggregation. For example 'summing' a value within a grouping can be performed partially Map side and
 * completed Reduce side. Summing is associative and commutative.
 * <p>
 * AggregateBy also supports operations that are not associative/commutative like 'counting'. Counting
 * would result in 'counting' value occurrences Map side but summing those counts Reduce side. (Yes, counting can be
 * transposed to summing Map and Reduce sides by emitting 1's before the first sum, but that's three operations over
 * two, and a hack)
 * <p>
 * Think of this mechanism as a MapReduce Combiner, but more efficient as no values are serialized,
 * deserialized, saved to disk, and multi-pass sorted in the process, which consume cpu resources in trade of
 * memory and a little or no IO.
 * <p>
 * Further, Combiners are limited to only associative/commutative operations.
 * <p>
 * Additionally the Cascading planner can move the Map side optimization
 * to the previous Reduce operation further increasing IO performance (between the preceding Reduce and Map phase which
 * is over HDFS).
 * <p>
 * The second role of the AggregateBy class is to allow for composition of AggregateBy
 * sub-classes. That is, {@link SumBy} and {@link CountBy} AggregateBy sub-classes can be performed
 * in parallel on the same grouping keys.
 * <p>
 * Custom AggregateBy classes can be created by sub-classing this class and implementing a special
 * {@link Functor} for use on the Map side. Multiple Functor instances are managed by the {@link CompositeFunction}
 * class allowing them all to share the same LRU value map for more efficiency.
 * <p>
 * AggregateBy instances return {@code argumentFields} which are used internally to control the values passed to
 * internal Functor instances. If any argumentFields also have {@link java.util.Comparator}s, they will be used
 * to for secondary sorting (see {@link GroupBy} {@code sortFields}. This feature is used by {@link FirstBy} to
 * control which Tuple is seen first for a grouping.
 * <p>
 * To tune the LRU, set the {@code capacity} value to a high enough value to utilize available memory. Or set a
 * default value via the {@link cascading.pipe.assembly.AggregateByProps#AGGREGATE_BY_CAPACITY} property. The current default
 * ({@link cascading.util.cache.BaseCacheFactory#DEFAULT_CAPACITY})
 * is {@code 10, 000} unique keys.
 * <p>
 * The LRU cache is pluggable and defaults to {@link cascading.util.cache.LRUHashMapCache}. It can be changed
 * by setting {@link cascading.pipe.assembly.AggregateByProps#AGGREGATE_BY_CACHE_FACTORY} property to the name of a sub-class of
 * {@link cascading.util.cache.BaseCacheFactory}.
 * <p>
 * Note using a AggregateBy instance automatically inserts a {@link GroupBy} into the resulting {@link cascading.flow.Flow}.
 * And passing multiple AggregateBy instances to a parent AggregateBy instance still results in one GroupBy.
 * <p>
 * Also note that {@link Unique} is not a CompositeAggregator and is slightly more optimized internally.
 * <p>
 * As of Cascading 2.6 AggregateBy honors the {@link cascading.tuple.Hasher} interface for storing keys in the cache.
 *
 * @see SumBy
 * @see CountBy
 * @see Unique
 * @see cascading.util.cache.LRUHashMapCacheFactory
 * @see cascading.util.cache.DirectMappedCacheFactory
 * @see cascading.util.cache.LRUHashMapCache
 * @see cascading.util.cache.DirectMappedCache
 */
public class AggregateBy extends SubAssembly
  {
  public static final int USE_DEFAULT_THRESHOLD = 0;

  public enum Cache
    {
      Num_Keys_Flushed,
      Num_Keys_Hit,
      Num_Keys_Missed
    }

  /**
   * Interface Functor provides a means to create a simple function for use with the {@link CompositeFunction} class.
   * <p>
   * Note the {@link FlowProcess} argument provides access to the underlying properties and counter APIs.
   */
  public interface Functor extends cascading.operation.CompositeFunction.CoFunction
    {
    }

  /**
   * Class CompositeFunction takes multiple Functor instances and manages them as a single {@link Function}.
   *
   * @see CoFunction
   */
  public static class CompositeFunction extends cascading.operation.CompositeFunction
    {
    public CompositeFunction( Fields groupingFields, Fields argumentFields, CoFunction coFunction, int capacity )
      {
      super( groupingFields, argumentFields, coFunction, capacity );
      }

    public CompositeFunction( Fields groupingFields, Fields[] argumentFields, CoFunction[] coFunctions, int capacity )
      {
      super( groupingFields, argumentFields, coFunctions, capacity );
      }

    protected void incrementNumKeysFlushed( FlowProcess flowProcess )
      {
      flowProcess.increment( AggregateBy.Cache.Num_Keys_Flushed, 1 );
      }

    protected void incrementNumKeysHit( FlowProcess flowProcess )
      {
      flowProcess.increment( AggregateBy.Cache.Num_Keys_Hit, 1 );
      }

    protected void incrementNumKeysMissed( FlowProcess flowProcess )
      {
      flowProcess.increment( AggregateBy.Cache.Num_Keys_Missed, 1 );
      }

    protected Integer getCacheCapacity( FlowProcess flowProcess )
      {
      return getCacheCapacity( flowProcess, AggregateByProps.AGGREGATE_BY_CAPACITY, AggregateByProps.AGGREGATE_BY_DEFAULT_CAPACITY );
      }

    protected BaseCacheFactory<Tuple, Tuple[], ?> loadCacheFactory( FlowProcess flowProcess )
      {
      return loadCacheFactory( flowProcess, AggregateByProps.AGGREGATE_BY_CACHE_FACTORY, AggregateByProps.DEFAULT_CACHE_FACTORY_CLASS );
      }
    }

  private String name;
  private int capacity;
  private Fields groupingFields;
  private Fields[] argumentFields;
  private Functor[] functors;
  private Aggregator[] aggregators;
  private transient GroupBy groupBy;

  /**
   * Constructor AggregateBy creates a new AggregateBy instance.
   *
   * @param name     of type String
   * @param capacity of type int
   */
  protected AggregateBy( String name, int capacity )
    {
    this.name = name;
    this.capacity = capacity;
    }

  /**
   * Constructor AggregateBy creates a new AggregateBy instance.
   *
   * @param argumentFields of type Fields
   * @param functor        of type Functor
   * @param aggregator     of type Aggregator
   */
  protected AggregateBy( Fields argumentFields, Functor functor, Aggregator aggregator )
    {
    this.argumentFields = Fields.fields( argumentFields );
    this.functors = new Functor[]{functor};
    this.aggregators = new Aggregator[]{aggregator};
    }

  /**
   * Constructor AggregateBy creates a new AggregateBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param assemblies     of type AggregateBy...
   */
  @ConstructorProperties({"pipe", "groupingFields", "assemblies"})
  public AggregateBy( Pipe pipe, Fields groupingFields, AggregateBy... assemblies )
    {
    this( null, Pipe.pipes( pipe ), groupingFields, 0, assemblies );
    }

  /**
   * Constructor AggregateBy creates a new AggregateBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param capacity       of type int
   * @param assemblies     of type AggregateBy...
   */
  @ConstructorProperties({"pipe", "groupingFields", "capacity", "assemblies"})
  public AggregateBy( Pipe pipe, Fields groupingFields, int capacity, AggregateBy... assemblies )
    {
    this( null, Pipe.pipes( pipe ), groupingFields, capacity, assemblies );
    }

  /**
   * Constructor AggregateBy creates a new AggregateBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param capacity       of type int
   * @param assemblies     of type AggregateBy...
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "capacity", "assemblies"})
  public AggregateBy( String name, Pipe pipe, Fields groupingFields, int capacity, AggregateBy... assemblies )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, capacity, assemblies );
    }

  /**
   * Constructor AggregateBy creates a new AggregateBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param assemblies     of type AggregateBy...
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "assemblies"})
  public AggregateBy( String name, Pipe[] pipes, Fields groupingFields, AggregateBy... assemblies )
    {
    this( name, pipes, groupingFields, 0, assemblies );
    }

  /**
   * Constructor AggregateBy creates a new AggregateBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param capacity       of type int
   * @param assemblies     of type AggregateBy...
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "capacity", "assemblies"})
  public AggregateBy( String name, Pipe[] pipes, Fields groupingFields, int capacity, AggregateBy... assemblies )
    {
    this( name, capacity );

    List<Fields> arguments = new ArrayList<>();
    List<Functor> functors = new ArrayList<>();
    List<Aggregator> aggregators = new ArrayList<>();

    for( int i = 0; i < assemblies.length; i++ )
      {
      AggregateBy assembly = assemblies[ i ];

      Collections.addAll( arguments, assembly.getArgumentFields() );
      Collections.addAll( functors, assembly.getFunctors() );
      Collections.addAll( aggregators, assembly.getAggregators() );
      }

    initialize( groupingFields, pipes, arguments.toArray( new Fields[ arguments.size() ] ), functors.toArray( new Functor[ functors.size() ] ), aggregators.toArray( new Aggregator[ aggregators.size() ] ) );
    }

  protected AggregateBy( String name, Pipe[] pipes, Fields groupingFields, Fields argumentFields, Functor functor, Aggregator aggregator, int capacity )
    {
    this( name, capacity );
    initialize( groupingFields, pipes, argumentFields, functor, aggregator );
    }

  protected void initialize( Fields groupingFields, Pipe[] pipes, Fields argumentFields, Functor functor, Aggregator aggregator )
    {
    initialize( groupingFields, pipes, Fields.fields( argumentFields ),
      new Functor[]{functor},
      new Aggregator[]{aggregator} );
    }

  protected void initialize( Fields groupingFields, Pipe[] pipes, Fields[] argumentFields, Functor[] functors, Aggregator[] aggregators )
    {
    setPrevious( pipes );

    this.groupingFields = groupingFields;
    this.argumentFields = argumentFields;
    this.functors = functors;
    this.aggregators = aggregators;

    verify();

    Fields sortFields = Fields.copyComparators( Fields.merge( this.argumentFields ), this.argumentFields );
    Fields argumentSelector = Fields.merge( this.groupingFields, sortFields );

    if( argumentSelector.equals( Fields.NONE ) )
      argumentSelector = Fields.ALL;

    Pipe[] functions = new Pipe[ pipes.length ];

    CompositeFunction function = new CompositeFunction( this.groupingFields, this.argumentFields, this.functors, capacity );

    for( int i = 0; i < functions.length; i++ )
      functions[ i ] = new Each( pipes[ i ], argumentSelector, function, Fields.RESULTS );

    groupBy = new GroupBy( name, functions, this.groupingFields, sortFields.hasComparators() ? sortFields : null );

    Pipe pipe = groupBy;

    for( int i = 0; i < aggregators.length; i++ )
      pipe = new Every( pipe, this.functors[ i ].getDeclaredFields(), this.aggregators[ i ], Fields.ALL );

    setTails( pipe );
    }

  /** Method verify should be overridden by sub-classes if any values must be tested before the calling constructor returns. */
  protected void verify()
    {

    }

  /**
   * Method getGroupingFields returns the Fields this instances will be grouping against.
   *
   * @return the current grouping fields
   */
  public Fields getGroupingFields()
    {
    return groupingFields;
    }

  /**
   * Method getFieldDeclarations returns an array of Fields where each Field element in the array corresponds to the
   * field declaration of the given Aggregator operations.
   * <p>
   * Note the actual Fields values are returned, not planner resolved Fields.
   *
   * @return and array of Fields
   */
  public Fields[] getFieldDeclarations()
    {
    Fields[] fields = new Fields[ this.aggregators.length ];

    for( int i = 0; i < aggregators.length; i++ )
      fields[ i ] = aggregators[ i ].getFieldDeclaration();

    return fields;
    }

  protected Fields[] getArgumentFields()
    {
    return argumentFields;
    }

  protected Functor[] getFunctors()
    {
    return functors;
    }

  protected Aggregator[] getAggregators()
    {
    return aggregators;
    }

  /**
   * Method getGroupBy returns the internal {@link GroupBy} instance so that any custom properties
   * can be set on it via {@link cascading.pipe.Pipe#getStepConfigDef()}.
   *
   * @return GroupBy type
   */
  public GroupBy getGroupBy()
    {
    return groupBy;
    }

  @Property(name = "capacity", visibility = Visibility.PUBLIC)
  @PropertyDescription("Capacity of the aggregation cache.")
  @PropertyConfigured(value = AggregateByProps.AGGREGATE_BY_CAPACITY, defaultValue = "10000")
  public int getCapacity()
    {
    return capacity;
    }
  }
