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

package cascading.operation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.provider.FactoryLoader;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.util.TupleHasher;
import cascading.tuple.util.TupleViews;
import cascading.util.cache.BaseCacheFactory;
import cascading.util.cache.CacheEvictionCallback;
import cascading.util.cache.CascadingCache;
import cascading.util.cache.LRUHashMapCacheFactory;

/**
 * Class CompositeFunction is a {@link Function} implementation that will apply multiple CoFunctions to a single
 * grouping of keys, where the partial results are maintained in a cache having a given eviction policy. On eviction
 * the CoFunctions for the evicted grouping key can complete any partial aggregations.
 * <p>
 * This class powers the {@link cascading.pipe.assembly.AggregateBy} partial aggregation functor composition.
 */
public class CompositeFunction extends BaseOperation<CompositeFunction.Context> implements Function<CompositeFunction.Context>
  {
  /** property to control the capacity of the cache to use. */
  public static final String COMPOSITE_FUNCTION_CAPACITY = "cascading.function.composite.cache.capacity";
  /** default factory class for creating caches. */
  public static final Class<? extends BaseCacheFactory> DEFAULT_CACHE_FACTORY_CLASS = LRUHashMapCacheFactory.class;
  /** property to control the cache factory used in this CompositeFunction. */
  public static String COMPOSITE_FUNCTION_CACHE_FACTORY = "cascading.function.composite.cachefactory.classname";
  /** default capacity of caches used in this CompositeFunction. */
  public static int COMPOSITE_FUNCTION_DEFAULT_CAPACITY = BaseCacheFactory.DEFAULT_CAPACITY;

  public enum Cache
    {
      Num_Keys_Flushed,
      Num_Keys_Hit,
      Num_Keys_Missed
    }

  /**
   *
   */
  public interface CoFunction extends Serializable
    {
    /**
     * Method getDeclaredFields returns the declaredFields of this CoFunction object.
     *
     * @return the declaredFields (type Fields) of this CoFunction object.
     */
    Fields getDeclaredFields();

    /**
     * Method aggregate operates on the given args in tandem (optionally) with the given context values.
     * <p>
     * The context argument is the result of the previous call to this method. Use it to store values between aggregate
     * calls (the current count, or sum of the args).
     * <p>
     * On the very first invocation of aggregate for a given grouping key, context will be {@code null}. All subsequent
     * invocations context will be the value returned on the previous invocation.
     *
     * @param flowProcess of type FlowProcess
     * @param args        of type TupleEntry
     * @param context     of type Tuple   @return Tuple
     */
    Tuple aggregate( FlowProcess flowProcess, TupleEntry args, Tuple context );

    /**
     * Method complete allows the final aggregate computation to be performed before the return value is collected.
     * <p>
     * The number of values in the returned {@link Tuple} instance must match the number of declaredFields.
     * <p>
     * It is safe to return the context object as the result value.
     *
     * @param flowProcess of type FlowProcess
     * @param context     of type Tuple  @return Tuple
     */
    Tuple complete( FlowProcess flowProcess, Tuple context );
    }

  public static class Context
    {
    CascadingCache<Tuple, Tuple[]> lru;
    TupleEntry[] arguments;
    Tuple result;
    }

  private final Fields groupingFields;
  private final Fields[] argumentFields;
  private final Fields[] functorFields;
  private final CoFunction[] coFunctions;
  private final TupleHasher tupleHasher;
  private int capacity = 0;

  /**
   * Constructor CompositeFunction creates a new CompositeFunction instance.
   *
   * @param groupingFields of type Fields
   * @param argumentFields of type Fields
   * @param coFunction     of type Functor
   * @param capacity       of type int
   */
  public CompositeFunction( Fields groupingFields, Fields argumentFields, CoFunction coFunction, int capacity )
    {
    this( groupingFields, Fields.fields( argumentFields ), new CoFunction[]{coFunction}, capacity );
    }

  /**
   * Constructor CompositeFunction creates a new CompositeFunction instance.
   *
   * @param groupingFields of type Fields
   * @param argumentFields of type Fields[]
   * @param coFunctions    of type Functor[]
   * @param capacity       of type int
   */
  public CompositeFunction( Fields groupingFields, Fields[] argumentFields, CoFunction[] coFunctions, int capacity )
    {
    super( getFields( groupingFields, coFunctions ) ); // todo: groupingFields should lookup incoming type information
    this.groupingFields = groupingFields;
    this.argumentFields = argumentFields;
    this.coFunctions = coFunctions;
    this.capacity = capacity;

    this.functorFields = new Fields[ coFunctions.length ];

    for( int i = 0; i < coFunctions.length; i++ )
      this.functorFields[ i ] = coFunctions[ i ].getDeclaredFields();

    Comparator[] hashers = TupleHasher.merge( functorFields );

    if( !TupleHasher.isNull( hashers ) )
      this.tupleHasher = new TupleHasher( null, hashers );
    else
      this.tupleHasher = null;
    }

  private static Fields getFields( Fields groupingFields, CoFunction[] coFunctions )
    {
    Fields fields = groupingFields;

    for( CoFunction functor : coFunctions )
      fields = fields.append( functor.getDeclaredFields() );

    return fields;
    }

  @Override
  public void prepare( final FlowProcess flowProcess, final OperationCall<CompositeFunction.Context> operationCall )
    {
    Fields[] fields = new Fields[ coFunctions.length + 1 ];

    fields[ 0 ] = groupingFields;

    for( int i = 0; i < coFunctions.length; i++ )
      fields[ i + 1 ] = coFunctions[ i ].getDeclaredFields();

    final Context context = new Context();

    context.arguments = new TupleEntry[ coFunctions.length ];

    for( int i = 0; i < context.arguments.length; i++ )
      {
      Fields resolvedArgumentFields = operationCall.getArgumentFields();

      int[] pos;

      if( argumentFields[ i ].isAll() )
        pos = resolvedArgumentFields.getPos();
      else
        pos = resolvedArgumentFields.getPos( argumentFields[ i ] ); // returns null if selector is ALL

      Tuple narrow = TupleViews.createNarrow( pos );

      Fields currentFields;

      if( this.argumentFields[ i ].isSubstitution() )
        currentFields = resolvedArgumentFields.select( this.argumentFields[ i ] ); // attempt to retain comparator
      else
        currentFields = Fields.asDeclaration( this.argumentFields[ i ] );

      context.arguments[ i ] = new TupleEntry( currentFields, narrow );
      }

    context.result = TupleViews.createComposite( fields );

    class Eviction implements CacheEvictionCallback<Tuple, Tuple[]>
      {
      @Override
      public void evict( Map.Entry<Tuple, Tuple[]> entry )
        {
        completeFunctors( flowProcess, ( (FunctionCall) operationCall ).getOutputCollector(), context.result, entry );
        incrementNumKeysFlushed( flowProcess );
        }
      }

    BaseCacheFactory<Tuple, Tuple[], ?> factory = loadCacheFactory( flowProcess );

    CascadingCache<Tuple, Tuple[]> cache = factory.create( flowProcess );

    cache.setCacheEvictionCallback( new Eviction() );

    Integer cacheCapacity = capacity;

    if( capacity == 0 )
      cacheCapacity = getCacheCapacity( flowProcess );

    cache.setCapacity( cacheCapacity.intValue() );
    cache.initialize();

    context.lru = cache;

    operationCall.setContext( context );
    }

  protected Integer getCacheCapacity( FlowProcess flowProcess )
    {
    return getCacheCapacity( flowProcess, COMPOSITE_FUNCTION_CAPACITY, COMPOSITE_FUNCTION_DEFAULT_CAPACITY );
    }

  protected BaseCacheFactory<Tuple, Tuple[], ?> loadCacheFactory( FlowProcess flowProcess )
    {
    return loadCacheFactory( flowProcess, COMPOSITE_FUNCTION_CACHE_FACTORY, DEFAULT_CACHE_FACTORY_CLASS );
    }

  protected Integer getCacheCapacity( FlowProcess flowProcess, String property, int defaultValue )
    {
    Integer cacheCapacity = flowProcess.getIntegerProperty( property );

    if( cacheCapacity == null )
      cacheCapacity = defaultValue;

    return cacheCapacity;
    }

  protected BaseCacheFactory<Tuple, Tuple[], ?> loadCacheFactory( FlowProcess flowProcess, String property, Class<? extends BaseCacheFactory> type )
    {
    FactoryLoader loader = FactoryLoader.getInstance();
    BaseCacheFactory<Tuple, Tuple[], ?> factory = loader.loadFactoryFrom( flowProcess, property, type );

    if( factory == null )
      throw new CascadingException( "unable to load cache factory, please check your '" + property + "' setting." );

    return factory;
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<CompositeFunction.Context> functionCall )
    {
    TupleEntry arguments = functionCall.getArguments();
    Tuple key = TupleHasher.wrapTuple( this.tupleHasher, arguments.selectTupleCopy( groupingFields ) );

    Context context = functionCall.getContext();
    Tuple[] functorContext = context.lru.get( key );

    if( functorContext == null )
      {
      functorContext = new Tuple[ coFunctions.length ];
      context.lru.put( key, functorContext );
      incrementNumKeysMissed( flowProcess );
      }
    else
      {
      incrementNumKeysHit( flowProcess );
      }

    for( int i = 0; i < coFunctions.length; i++ )
      {
      TupleViews.reset( context.arguments[ i ].getTuple(), arguments.getTuple() );
      functorContext[ i ] = coFunctions[ i ].aggregate( flowProcess, context.arguments[ i ], functorContext[ i ] );
      }
    }

  protected void incrementNumKeysFlushed( FlowProcess flowProcess )
    {
    flowProcess.increment( Cache.Num_Keys_Flushed, 1 );
    }

  protected void incrementNumKeysHit( FlowProcess flowProcess )
    {
    flowProcess.increment( Cache.Num_Keys_Hit, 1 );
    }

  protected void incrementNumKeysMissed( FlowProcess flowProcess )
    {
    flowProcess.increment( Cache.Num_Keys_Missed, 1 );
    }

  @Override
  public void flush( FlowProcess flowProcess, OperationCall<CompositeFunction.Context> operationCall )
    {
    // need to drain context
    TupleEntryCollector collector = ( (FunctionCall) operationCall ).getOutputCollector();

    Tuple result = operationCall.getContext().result;
    Map<Tuple, Tuple[]> context = operationCall.getContext().lru;

    for( Map.Entry<Tuple, Tuple[]> entry : context.entrySet() )
      completeFunctors( flowProcess, collector, result, entry );

    context.clear();
    }

  @Override
  public void cleanup( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    operationCall.setContext( null );
    }

  private void completeFunctors( FlowProcess flowProcess, TupleEntryCollector outputCollector, Tuple result, Map.Entry<Tuple, Tuple[]> entry )
    {
    Tuple[] results = new Tuple[ coFunctions.length + 1 ];

    results[ 0 ] = entry.getKey();

    Tuple[] values = entry.getValue();

    for( int i = 0; i < coFunctions.length; i++ )
      results[ i + 1 ] = coFunctions[ i ].complete( flowProcess, values[ i ] );

    TupleViews.reset( result, results );

    outputCollector.add( result );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof CompositeFunction ) )
      return false;
    if( !super.equals( object ) )
      return false;

    CompositeFunction that = (CompositeFunction) object;

    if( !Arrays.equals( argumentFields, that.argumentFields ) )
      return false;
    if( !Arrays.equals( functorFields, that.functorFields ) )
      return false;
    if( !Arrays.equals( coFunctions, that.coFunctions ) )
      return false;
    if( groupingFields != null ? !groupingFields.equals( that.groupingFields ) : that.groupingFields != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( groupingFields != null ? groupingFields.hashCode() : 0 );
    result = 31 * result + ( argumentFields != null ? Arrays.hashCode( argumentFields ) : 0 );
    result = 31 * result + ( functorFields != null ? Arrays.hashCode( functorFields ) : 0 );
    result = 31 * result + ( coFunctions != null ? Arrays.hashCode( coFunctions ) : 0 );
    return result;
    }
  }
