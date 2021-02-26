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
import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.cache.BaseCacheFactory;

/**
 * Class AggregateByLocally is a {@link SubAssembly} handles locally aggregate operations.
 * <p>
 * Locally aggregate operations are aggregate functions applied to a common key where the key and partial aggregations
 * are kept in a evictable cache. On eviction of the unique (or when the stream completes), any associated partial
 * aggregates are completed and then emitted into the tuple stream.
 * <p>
 * If they key is seen again after being evicted, the aggregation will start again, resulting in duplicate copies of the
 * keys with their associated (now) partial aggregation.
 * <p>
 * Subsequently, this function is not guaranteed to aggregate all values of a unique key together. See
 * {@link AggregateBy} for such a guarantee.
 * <p>
 * Use this class when all values of a unique key are expected to be local to each other, which is not a property
 * MapReduce applications can guarantee due to file splitting by default. Cascading local mode, using only a single
 * thread, can make this guarantee if the values are retrieved from the source having locally arranged keys.
 * <p>
 * The AggregateByLocally class allows for composition of AggregateByLocally
 * sub-classes. That is, {@link SumByLocally} and {@link CountByLocally} AggregateByLocally sub-classes can be performed
 * in parallel on the same grouping keys.
 * <p>
 * Custom AggregateBy classes can be created by sub-classing this class and implementing a special
 * {@link Functor}. Multiple Functor instances are managed by the {@link CompositeFunction}
 * class allowing them all to share the same LRU value map for more efficiency.
 * <p>
 * To tune the LRU, set the {@code capacity} value to a high enough value to utilize available memory. Or set a
 * default value via the {@link AggregateByLocallyProps#AGGREGATE_LOCALLY_BY_CAPACITY} property. The current default
 * ({@link BaseCacheFactory#DEFAULT_CAPACITY})
 * is {@code 10, 000} unique keys.
 * <p>
 * The LRU cache is pluggable and defaults to {@link cascading.util.cache.LRUHashMapCache}. It can be changed
 * by setting {@link AggregateByLocallyProps#AGGREGATE_LOCALLY_BY_CACHE_FACTORY} property to the name of a sub-class of
 * {@link BaseCacheFactory}.
 * <p>
 * Note using a AggregateByLocally instance does not insert a {@link GroupBy} into the resulting {@link cascading.flow.Flow}.
 *
 * @see SumByLocally
 * @see cascading.util.cache.LRUHashMapCacheFactory
 * @see cascading.util.cache.DirectMappedCacheFactory
 * @see cascading.util.cache.LRUHashMapCache
 * @see cascading.util.cache.DirectMappedCache
 */
public class AggregateByLocally extends SubAssembly
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
      flowProcess.increment( AggregateByLocally.Cache.Num_Keys_Flushed, 1 );
      }

    protected void incrementNumKeysHit( FlowProcess flowProcess )
      {
      flowProcess.increment( AggregateByLocally.Cache.Num_Keys_Hit, 1 );
      }

    protected void incrementNumKeysMissed( FlowProcess flowProcess )
      {
      flowProcess.increment( AggregateByLocally.Cache.Num_Keys_Missed, 1 );
      }

    protected Integer getCacheCapacity( FlowProcess flowProcess )
      {
      return getCacheCapacity( flowProcess, AggregateByLocallyProps.AGGREGATE_LOCALLY_BY_CAPACITY, AggregateByLocallyProps.AGGREGATE_LOCALLY_BY_DEFAULT_CAPACITY );
      }

    protected BaseCacheFactory<Tuple, Tuple[], ?> loadCacheFactory( FlowProcess flowProcess )
      {
      return loadCacheFactory( flowProcess, AggregateByLocallyProps.AGGREGATE_LOCALLY_BY_CACHE_FACTORY, AggregateByLocallyProps.DEFAULT_CACHE_FACTORY_CLASS );
      }
    }

  private String name;
  private int capacity;
  private Fields groupingFields;
  private Fields[] argumentFields;
  private Functor[] functors;

  /**
   * Constructor AggregateBy creates a new AggregateBy instance.
   *
   * @param name     of type String
   * @param capacity of type int
   */
  protected AggregateByLocally( String name, int capacity )
    {
    this.name = name;
    this.capacity = capacity;
    }

  /**
   * Constructor AggregateBy creates a new AggregateBy instance.
   *
   * @param argumentFields of type Fields
   * @param functor        of type Functor
   */
  protected AggregateByLocally( Fields argumentFields, Functor functor )
    {
    this.argumentFields = Fields.fields( argumentFields );
    this.functors = new Functor[]{functor};
    }

  /**
   * Constructor AggregateBy creates a new AggregateBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param assemblies     of type AggregateBy...
   */
  @ConstructorProperties({"pipe", "groupingFields", "assemblies"})
  public AggregateByLocally( Pipe pipe, Fields groupingFields, AggregateByLocally... assemblies )
    {
    this( null, pipe, groupingFields, 0, assemblies );
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
  public AggregateByLocally( Pipe pipe, Fields groupingFields, int capacity, AggregateByLocally... assemblies )
    {
    this( null, pipe, groupingFields, capacity, assemblies );
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
  public AggregateByLocally( String name, Pipe pipe, Fields groupingFields, int capacity, AggregateByLocally... assemblies )
    {
    this( name, capacity );

    List<Fields> arguments = new ArrayList<>();
    List<Functor> functors = new ArrayList<>();

    for( int i = 0; i < assemblies.length; i++ )
      {
      AggregateByLocally assembly = assemblies[ i ];

      Collections.addAll( arguments, assembly.getArgumentFields() );
      Collections.addAll( functors, assembly.getFunctors() );
      }

    initialize( groupingFields, pipe, arguments.toArray( new Fields[ arguments.size() ] ), functors.toArray( new Functor[ functors.size() ] ) );
    }

  protected AggregateByLocally( String name, Pipe pipe, Fields groupingFields, Fields argumentFields, Functor functor, int capacity )
    {
    this( name, capacity );
    initialize( groupingFields, pipe, argumentFields, functor );
    }

  protected void initialize( Fields groupingFields, Pipe pipe, Fields argumentFields, Functor functor )
    {
    initialize( groupingFields, pipe, Fields.fields( argumentFields ), new Functor[]{functor} );
    }

  protected void initialize( Fields groupingFields, Pipe pipe, Fields[] argumentFields, Functor[] functors )
    {
    setPrevious( pipe );

    this.groupingFields = groupingFields;
    this.argumentFields = argumentFields;
    this.functors = functors;

    verify();

    Fields sortFields = Fields.copyComparators( Fields.merge( this.argumentFields ), this.argumentFields );
    Fields argumentSelector = Fields.merge( this.groupingFields, sortFields );

    if( argumentSelector.equals( Fields.NONE ) )
      argumentSelector = Fields.ALL;

    CompositeFunction function = new CompositeFunction( this.groupingFields, this.argumentFields, this.functors, capacity );

    if( name != null )
      pipe = new Pipe( name );

    pipe = new Each( pipe, argumentSelector, function, Fields.RESULTS );

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
   * field declaration of the given Functor.
   * <p>
   * Note the actual Fields values are returned, not planner resolved Fields.
   *
   * @return and array of Fields
   */
  public Fields[] getFieldDeclarations()
    {
    Fields[] fields = new Fields[ this.functors.length ];

    for( int i = 0; i < functors.length; i++ )
      fields[ i ] = functors[ i ].getDeclaredFields();

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

  @Property(name = "capacity", visibility = Visibility.PUBLIC)
  @PropertyDescription("Capacity of the aggregation cache.")
  @PropertyConfigured(value = AggregateByLocallyProps.AGGREGATE_LOCALLY_BY_CAPACITY, defaultValue = "10000")
  public int getCapacity()
    {
    return capacity;
    }
  }
