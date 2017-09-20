/*
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
import java.util.Comparator;
import java.util.Map;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.operation.buffer.FirstNBuffer;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.provider.FactoryLoader;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;
import cascading.tuple.util.TupleHasher;
import cascading.util.cache.BaseCacheFactory;
import cascading.util.cache.CacheEvictionCallback;
import cascading.util.cache.CascadingCache;

/**
 * Class Unique {@link SubAssembly} is used to filter all duplicates out of a tuple stream.
 * <p/>
 * Typically finding unique value in a tuple stream relies on a {@link GroupBy} and a {@link FirstNBuffer}
 * {@link cascading.operation.Buffer} operation.
 * <p/>
 * If the {@code include} value is set to {@link Include#NO_NULLS}, any tuple consisting of only {@code null}
 * values will be removed from the stream.
 * <p/>
 * This SubAssembly uses the {@link FilterPartialDuplicates} {@link cascading.operation.Filter}
 * to remove as many observed duplicates before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * Unique uses a {@link cascading.util.cache.CascadingCache} or LRU to do the filtering. To tune the cache, set the
 * {@code capacity} value to a high enough value to utilize available memory. Or set a default value via the
 * {@link cascading.pipe.assembly.UniqueProps#UNIQUE_CACHE_CAPACITY} property. The current default is {@code 10, 000} unique keys.
 * <p/>
 * The LRU cache is pluggable and defaults to {@link cascading.util.cache.LRUHashMapCache}. It can be changed
 * by setting {@link cascading.pipe.assembly.UniqueProps#UNIQUE_CACHE_FACTORY} property to the name of a sub-class of
 * {@link cascading.util.cache.BaseCacheFactory}.
 * <p/>
 * The {@code capacity} value tells the underlying FilterPartialDuplicates how many values to cache for duplicate
 * comparison before dropping values from the LRU cache.
 *
 * @see cascading.util.cache.LRUHashMapCacheFactory
 * @see cascading.util.cache.DirectMappedCacheFactory
 * @see cascading.util.cache.LRUHashMapCache
 * @see cascading.util.cache.DirectMappedCache
 */
public class Unique extends SubAssembly
  {

  public enum Include
    {
      ALL,
      NO_NULLS
    }

  public enum Cache
    {
      Num_Keys_Flushed,
      Num_Keys_Hit,
      Num_Keys_Missed
    }

  /**
   * Class FilterPartialDuplicates is a {@link cascading.operation.Filter} that is used to remove observed duplicates from the tuple stream.
   * <p/>
   * Use this class typically in tandem with a {@link cascading.operation.aggregator.First}
   * {@link cascading.operation.Aggregator} in order to improve de-duping performance by removing as many values
   * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
   * <p/>
   * The {@code capacity} value is used to maintain a LRU of a constant size. If more than capacity unique values
   * are seen, the oldest cached values will be removed from the cache.
   *
   * @see Unique
   */
  public static class FilterPartialDuplicates extends BaseOperation<CascadingCache<Tuple, Object>> implements Filter<CascadingCache<Tuple, Object>>
    {
    /** special null value for the caches, since a cache might not permit 'null' as a value */
    private final static Object NULL_VALUE = new Object();

    private int capacity = 0;
    private Include include = Include.ALL;
    private TupleHasher tupleHasher;

    /** Constructor FilterPartialDuplicates creates a new FilterPartialDuplicates instance. */
    public FilterPartialDuplicates()
      {
      }

    /**
     * Constructor FilterPartialDuplicates creates a new FilterPartialDuplicates instance.
     *
     * @param capacity of type int
     */
    @ConstructorProperties({"capacity"})
    public FilterPartialDuplicates( int capacity )
      {
      this.capacity = capacity;
      }

    /**
     * Constructor FilterPartialDuplicates creates a new FilterPartialDuplicates instance.
     *
     * @param include  of type Include
     * @param capacity of type int
     */
    @ConstructorProperties({"include", "capacity"})
    public FilterPartialDuplicates( Include include, int capacity )
      {
      this( include, capacity, null );
      }

    /**
     * Constructor FilterPartialDuplicates creates a new FilterPartialDuplicates instance.
     *
     * @param capacity    of type int
     * @param include     of type Include
     * @param tupleHasher of type TupleHasher
     */
    @ConstructorProperties({"include", "capacity", "tupleHasher"})
    public FilterPartialDuplicates( Include include, int capacity, TupleHasher tupleHasher )
      {
      this.capacity = capacity;
      this.include = include == null ? this.include : include;
      this.tupleHasher = tupleHasher;
      }

    @Override
    public void prepare( final FlowProcess flowProcess, OperationCall<CascadingCache<Tuple, Object>> operationCall )
      {
      CacheEvictionCallback callback = new CacheEvictionCallback()
        {
        @Override
        public void evict( Map.Entry entry )
          {
          flowProcess.increment( Cache.Num_Keys_Flushed, 1 );
          }
        };
      FactoryLoader loader = FactoryLoader.getInstance();
      BaseCacheFactory cacheFactory = loader.loadFactoryFrom( flowProcess, UniqueProps.UNIQUE_CACHE_FACTORY, UniqueProps.DEFAULT_CACHE_FACTORY_CLASS );

      if( cacheFactory == null )
        throw new CascadingException( "unable to load cache factory, please check your '" + UniqueProps.UNIQUE_CACHE_FACTORY + "' setting." );

      CascadingCache cache = cacheFactory.create( flowProcess );
      cache.setCacheEvictionCallback( callback );
      Integer cacheCapacity = capacity;

      if( capacity == 0 )
        {
        cacheCapacity = flowProcess.getIntegerProperty( UniqueProps.UNIQUE_CACHE_CAPACITY );

        if( cacheCapacity == null )
          cacheCapacity = UniqueProps.UNIQUE_DEFAULT_CAPACITY;
        }

      cache.setCapacity( cacheCapacity.intValue() );
      cache.initialize();

      operationCall.setContext( cache );
      }

    @Override
    public boolean isRemove( FlowProcess flowProcess, FilterCall<CascadingCache<Tuple, Object>> filterCall )
      {
      // we assume its more painful to create lots of tuple copies vs comparisons
      Tuple args = TupleHasher.wrapTuple( tupleHasher, filterCall.getArguments().getTuple() );

      switch( include )
        {
        case ALL:
          break;

        case NO_NULLS:
          if( Tuples.frequency( args, null ) == args.size() )
            return true;

          break;
        }

      if( filterCall.getContext().containsKey( args ) )
        {
        flowProcess.increment( Cache.Num_Keys_Hit, 1 );
        return true;
        }

      // only do the copy here
      filterCall.getContext().put( TupleHasher.wrapTuple( tupleHasher, filterCall.getArguments().getTupleCopy() ), NULL_VALUE );

      flowProcess.increment( Cache.Num_Keys_Missed, 1 );

      return false;
      }

    @Override
    public void cleanup( FlowProcess flowProcess, OperationCall<CascadingCache<Tuple, Object>> operationCall )
      {
      operationCall.setContext( null );
      }

    @Override
    public boolean equals( Object object )
      {
      if( this == object )
        return true;
      if( !( object instanceof FilterPartialDuplicates ) )
        return false;
      if( !super.equals( object ) )
        return false;

      FilterPartialDuplicates that = (FilterPartialDuplicates) object;

      if( capacity != that.capacity )
        return false;

      return true;
      }

    @Override
    public int hashCode()
      {
      int result = super.hashCode();
      result = 31 * result + capacity;
      return result;
      }
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipe         of type Pipe
   * @param uniqueFields of type Fields
   */
  @ConstructorProperties({"pipe", "uniqueFields"})
  public Unique( Pipe pipe, Fields uniqueFields )
    {
    this( null, pipe, uniqueFields );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipe         of type Pipe
   * @param uniqueFields of type Fields
   * @param include      of type Include
   */
  @ConstructorProperties({"pipe", "uniqueFields", "include"})
  public Unique( Pipe pipe, Fields uniqueFields, Include include )
    {
    this( null, pipe, uniqueFields, include );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipe         of type Pipe
   * @param uniqueFields of type Fields
   * @param capacity     of type int
   */
  @ConstructorProperties({"pipe", "uniqueFields", "capacity"})
  public Unique( Pipe pipe, Fields uniqueFields, int capacity )
    {
    this( null, pipe, uniqueFields, capacity );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipe         of type Pipe
   * @param uniqueFields of type Fields
   * @param include      of type Include
   * @param capacity     of type int
   */
  @ConstructorProperties({"pipe", "uniqueFields", "include", "capacity"})
  public Unique( Pipe pipe, Fields uniqueFields, Include include, int capacity )
    {
    this( null, pipe, uniqueFields, include, capacity );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name         of type String
   * @param pipe         of type Pipe
   * @param uniqueFields of type Fields
   */
  @ConstructorProperties({"name", "pipe", "uniqueFields"})
  public Unique( String name, Pipe pipe, Fields uniqueFields )
    {
    this( name, pipe, uniqueFields, null );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name         of type String
   * @param pipe         of type Pipe
   * @param uniqueFields of type Fields
   * @param include      of type Include
   */
  @ConstructorProperties({"name", "pipe", "uniqueFields", "include"})
  public Unique( String name, Pipe pipe, Fields uniqueFields, Include include )
    {
    this( name, pipe, uniqueFields, include, 0 );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name         of type String
   * @param pipe         of type Pipe
   * @param uniqueFields of type Fields
   * @param capacity     of type int
   */
  @ConstructorProperties({"name", "pipe", "uniqueFields", "capacity"})
  public Unique( String name, Pipe pipe, Fields uniqueFields, int capacity )
    {
    this( name, Pipe.pipes( pipe ), uniqueFields, capacity );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name         of type String
   * @param pipe         of type Pipe
   * @param uniqueFields of type Fields
   * @param include      of type Include
   * @param capacity     of type int
   */
  @ConstructorProperties({"name", "pipe", "uniqueFields", "include", "capacity"})
  public Unique( String name, Pipe pipe, Fields uniqueFields, Include include, int capacity )
    {
    this( name, Pipe.pipes( pipe ), uniqueFields, include, capacity );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipes        of type Pipe[]
   * @param uniqueFields of type Fields
   */
  @ConstructorProperties({"pipes", "uniqueFields"})
  public Unique( Pipe[] pipes, Fields uniqueFields )
    {
    this( null, pipes, uniqueFields );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipes        of type Pipe[]
   * @param uniqueFields of type Fields
   * @param include      of type Include
   */
  @ConstructorProperties({"pipes", "uniqueFields", "include"})
  public Unique( Pipe[] pipes, Fields uniqueFields, Include include )
    {
    this( null, pipes, uniqueFields, include );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipes        of type Pipe[]
   * @param uniqueFields of type Fields
   * @param capacity     of type int
   */
  @ConstructorProperties({"pipes", "uniqueFields", "capacity"})
  public Unique( Pipe[] pipes, Fields uniqueFields, int capacity )
    {
    this( null, pipes, uniqueFields, capacity );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipes        of type Pipe[]
   * @param uniqueFields of type Fields
   * @param include      of type Include
   * @param capacity     of type int
   */
  @ConstructorProperties({"pipes", "uniqueFields", "include", "capacity"})
  public Unique( Pipe[] pipes, Fields uniqueFields, Include include, int capacity )
    {
    this( null, pipes, uniqueFields, include, capacity );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name         of type String
   * @param pipes        of type Pipe[]
   * @param uniqueFields of type Fields
   */
  @ConstructorProperties({"name", "pipes", "uniqueFields"})
  public Unique( String name, Pipe[] pipes, Fields uniqueFields )
    {
    this( name, pipes, uniqueFields, null );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name         of type String
   * @param pipes        of type Pipe[]
   * @param uniqueFields of type Fields
   * @param include      of type Include
   */
  @ConstructorProperties({"name", "pipes", "uniqueFields", "include"})
  public Unique( String name, Pipe[] pipes, Fields uniqueFields, Include include )
    {
    this( name, pipes, uniqueFields, include, 0 );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name         of type String
   * @param pipes        of type Pipe[]
   * @param uniqueFields of type Fields
   * @param capacity     of type int
   */
  @ConstructorProperties({"name", "pipes", "uniqueFields", "capacity"})
  public Unique( String name, Pipe[] pipes, Fields uniqueFields, int capacity )
    {
    this( name, pipes, uniqueFields, null, capacity );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name         of type String
   * @param pipes        of type Pipe[]
   * @param uniqueFields of type Fields
   * @param capacity     of type int
   */
  @ConstructorProperties({"name", "pipes", "uniqueFields", "include", "capacity"})
  public Unique( String name, Pipe[] pipes, Fields uniqueFields, Include include, int capacity )
    {
    super( pipes );

    if( uniqueFields == null )
      throw new IllegalArgumentException( "uniqueFields may not be null" );

    Pipe[] filters = new Pipe[ pipes.length ];

    TupleHasher tupleHasher = null;
    Comparator[] comparators = uniqueFields.getComparators();

    if( !TupleHasher.isNull( comparators ) )
      tupleHasher = new TupleHasher( null, comparators );

    FilterPartialDuplicates partialDuplicates = new FilterPartialDuplicates( include, capacity, tupleHasher );

    for( int i = 0; i < filters.length; i++ )
      filters[ i ] = new Each( pipes[ i ], uniqueFields, partialDuplicates );

    Pipe pipe = new GroupBy( name, filters, uniqueFields );
    pipe = new Every( pipe, Fields.ALL, new FirstNBuffer(), Fields.RESULTS );

    setTails( pipe );
    }
  }
