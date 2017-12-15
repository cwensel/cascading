/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.management.annotation.Property;
import cascading.management.annotation.PropertyConfigured;
import cascading.management.annotation.PropertyDescription;
import cascading.management.annotation.Visibility;
import cascading.operation.Aggregator;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
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

  private String name;
  private int capacity;
  private Fields groupingFields;
  private Fields[] argumentFields;
  private Functor[] functors;
  private Aggregator[] aggregators;
  private transient GroupBy groupBy;

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
  public interface Functor extends Serializable
    {
    /**
     * Method getDeclaredFields returns the declaredFields of this Functor object.
     *
     * @return the declaredFields (type Fields) of this Functor object.
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

  /**
   * Class CompositeFunction takes multiple Functor instances and manages them as a single {@link Function}.
   *
   * @see Functor
   */
  public static class CompositeFunction extends BaseOperation<CompositeFunction.Context> implements Function<CompositeFunction.Context>
    {
    private int capacity = 0;
    private final Fields groupingFields;
    private final Fields[] argumentFields;
    private final Fields[] functorFields;
    private final Functor[] functors;
    private final TupleHasher tupleHasher;

    public static class Context
      {
      CascadingCache<Tuple, Tuple[]> lru;
      TupleEntry[] arguments;
      Tuple result;
      }

    /**
     * Constructor CompositeFunction creates a new CompositeFunction instance.
     *
     * @param groupingFields of type Fields
     * @param argumentFields of type Fields
     * @param functor        of type Functor
     * @param capacity       of type int
     */
    public CompositeFunction( Fields groupingFields, Fields argumentFields, Functor functor, int capacity )
      {
      this( groupingFields, Fields.fields( argumentFields ), new Functor[]{functor}, capacity );
      }

    /**
     * Constructor CompositeFunction creates a new CompositeFunction instance.
     *
     * @param groupingFields of type Fields
     * @param argumentFields of type Fields[]
     * @param functors       of type Functor[]
     * @param capacity       of type int
     */
    public CompositeFunction( Fields groupingFields, Fields[] argumentFields, Functor[] functors, int capacity )
      {
      super( getFields( groupingFields, functors ) ); // todo: groupingFields should lookup incoming type information
      this.groupingFields = groupingFields;
      this.argumentFields = argumentFields;
      this.functors = functors;
      this.capacity = capacity;

      this.functorFields = new Fields[ functors.length ];

      for( int i = 0; i < functors.length; i++ )
        this.functorFields[ i ] = functors[ i ].getDeclaredFields();

      Comparator[] hashers = TupleHasher.merge( functorFields );
      if( !TupleHasher.isNull( hashers ) )
        this.tupleHasher = new TupleHasher( null, hashers );
      else
        this.tupleHasher = null;
      }

    private static Fields getFields( Fields groupingFields, Functor[] functors )
      {
      Fields fields = groupingFields;

      for( Functor functor : functors )
        fields = fields.append( functor.getDeclaredFields() );

      return fields;
      }

    @Override
    public void prepare( final FlowProcess flowProcess, final OperationCall<CompositeFunction.Context> operationCall )
      {
      Fields[] fields = new Fields[ functors.length + 1 ];

      fields[ 0 ] = groupingFields;

      for( int i = 0; i < functors.length; i++ )
        fields[ i + 1 ] = functors[ i ].getDeclaredFields();

      final Context context = new Context();

      context.arguments = new TupleEntry[ functors.length ];

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

      class AggregateByEviction implements CacheEvictionCallback<Tuple, Tuple[]>
        {
        @Override
        public void evict( Map.Entry<Tuple, Tuple[]> entry )
          {
          completeFunctors( flowProcess, ( (FunctionCall) operationCall ).getOutputCollector(), context.result, entry );
          flowProcess.increment( Cache.Num_Keys_Flushed, 1 );
          }
        }

      FactoryLoader loader = FactoryLoader.getInstance();

      BaseCacheFactory<Tuple, Tuple[], ?> factory = loader.loadFactoryFrom( flowProcess, AggregateByProps.AGGREGATE_BY_CACHE_FACTORY, AggregateByProps.DEFAULT_CACHE_FACTORY_CLASS );

      if( factory == null )
        throw new CascadingException( "unable to load cache factory, please check your '" + AggregateByProps.AGGREGATE_BY_CACHE_FACTORY + "' setting." );

      CascadingCache<Tuple, Tuple[]> cache = factory.create( flowProcess );

      cache.setCacheEvictionCallback( new AggregateByEviction() );

      Integer cacheCapacity = capacity;
      if( capacity == 0 )
        {
        cacheCapacity = flowProcess.getIntegerProperty( AggregateByProps.AGGREGATE_BY_CAPACITY );

        if( cacheCapacity == null )
          cacheCapacity = AggregateByProps.AGGREGATE_BY_DEFAULT_CAPACITY;
        }
      cache.setCapacity( cacheCapacity.intValue() );
      cache.initialize();

      context.lru = cache;

      operationCall.setContext( context );
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
        functorContext = new Tuple[ functors.length ];
        context.lru.put( key, functorContext );
        flowProcess.increment( Cache.Num_Keys_Missed, 1 );
        }
      else
        {
        flowProcess.increment( Cache.Num_Keys_Hit, 1 );
        }

      for( int i = 0; i < functors.length; i++ )
        {
        TupleViews.reset( context.arguments[ i ].getTuple(), arguments.getTuple() );
        functorContext[ i ] = functors[ i ].aggregate( flowProcess, context.arguments[ i ], functorContext[ i ] );
        }
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
      Tuple[] results = new Tuple[ functors.length + 1 ];

      results[ 0 ] = entry.getKey();

      Tuple[] values = entry.getValue();

      for( int i = 0; i < functors.length; i++ )
        results[ i + 1 ] = functors[ i ].complete( flowProcess, values[ i ] );

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
      if( !Arrays.equals( functors, that.functors ) )
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
      result = 31 * result + ( functors != null ? Arrays.hashCode( functors ) : 0 );
      return result;
      }
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
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
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
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
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param assemblies     of type CompositeAggregator...
   */
  @ConstructorProperties({"pipe", "groupingFields", "assemblies"})
  public AggregateBy( Pipe pipe, Fields groupingFields, AggregateBy... assemblies )
    {
    this( null, Pipe.pipes( pipe ), groupingFields, 0, assemblies );
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param capacity       of type int
   * @param assemblies     of type CompositeAggregator...
   */
  @ConstructorProperties({"pipe", "groupingFields", "capacity", "assemblies"})
  public AggregateBy( Pipe pipe, Fields groupingFields, int capacity, AggregateBy... assemblies )
    {
    this( null, Pipe.pipes( pipe ), groupingFields, capacity, assemblies );
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param capacity       of type int
   * @param assemblies     of type CompositeAggregator...
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "capacity", "assemblies"})
  public AggregateBy( String name, Pipe pipe, Fields groupingFields, int capacity, AggregateBy... assemblies )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, capacity, assemblies );
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param assemblies     of type CompositeAggregator...
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "assemblies"})
  public AggregateBy( String name, Pipe[] pipes, Fields groupingFields, AggregateBy... assemblies )
    {
    this( name, pipes, groupingFields, 0, assemblies );
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param capacity       of type int
   * @param assemblies     of type CompositeAggregator...
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "capacity", "assemblies"})
  public AggregateBy( String name, Pipe[] pipes, Fields groupingFields, int capacity, AggregateBy... assemblies )
    {
    this( name, capacity );

    List<Fields> arguments = new ArrayList<Fields>();
    List<Functor> functors = new ArrayList<Functor>();
    List<Aggregator> aggregators = new ArrayList<Aggregator>();

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
