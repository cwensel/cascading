/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.pipe.assembly;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowProcess;
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
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * Class CompositeAggregator is a {@link SubAssembly} that serves two roles for handling aggregate operations.
 * <p/>
 * The first role is as a base class for composable aggregate operations that have a MapReduce Map side optimization for the
 * Reduce side aggregation. For example 'summing' a value within a grouping can be performed partially Map side and
 * completed Reduce side. Summing is associative and commutative.
 * <p/>
 * CompositeAggregators also support operations that are not associative/commutative like 'counting'. Counting
 * would result in 'counting' value occurrences Map side but summing those counts Reduce side. (Yes, counting can be
 * transposed to summing Map and Reduce sides by emitting 1's before the first sum, but that's three operations over
 * two, and a hack)
 * <p/>
 * Think of this mechanism as a MapReduce Combiner, but more efficient as no values are serialized,
 * deserialized, saved to disk, and multi-pass sorted in the process, which consume cpu resources in trade of
 * memory and a little or no IO.
 * <p/>
 * Further, Combiners are limited to only associative/commutative operations.
 * <p/>
 * And Combiners are limited to being applied only Map side, where the Cascading planner can move the optimization
 * to the previous Reduce operation further increasing IO performance (between the preceding Reduce and Map phase which
 * is over HDFS).
 * <p/>
 * The second role of the CompositeAggregator class is to allow for composition of CompositeAggregator
 * sub-classes. That is, {@link Sum} and {@link Count} CompositeAggregator sub-classes can be performed
 * in parallel on the same grouping keys.
 * </p>
 * Custom CompositeAggregator classes can be created by sub-classing this class and implementing a special
 * {@link Functor} for use on the Map side. Multiple Functor instances are managed by the {@link CompositeFunction}
 * class allowing them all to share the same LRU value map for more efficiency.
 * <p/>
 * To tune the LRU, set the {@code threshold} value to a high enough value to utilize available memory.
 * <p/>
 * Note that {@link Unique} is not a CompositeAggregator as it makes no sense to combine it with other aggregators,
 * and so is slightly more optimized internally.
 *
 * @see Sum
 * @see Count
 */
public class CompositeAggregator extends SubAssembly
  {
  private String name;
  private int threshold;
  private Fields[] argumentFields;
  private Functor[] functors;
  private Aggregator[] aggregators;

  /**
   * Interface Functor provides a means to create a simple function for use with the {@link CompositeFunction} class.
   * <p/>
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
     * <p/>
     * The context argument is the result of the previous call to this method. Use it to store values between aggregate
     * calls (the current count, or sum of the args).
     * <p/>
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
     * <p/>
     * The number of values in the returned {@link Tuple} instance must match the number of declaredFields.
     * <p/>
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
  public static class CompositeFunction extends BaseOperation<LinkedHashMap<Tuple, Tuple[]>> implements Function<LinkedHashMap<Tuple, Tuple[]>>
    {
    public static final int DEFAULT_THRESHOLD = 10000;

    private int threshold = DEFAULT_THRESHOLD;
    private Fields groupingFields;
    private Fields[] argumentFields;
    private Fields[] functorFields;
    private Functor[] functors;

    /**
     * Constructor CompositeFunction creates a new CompositeFunction instance.
     *
     * @param groupingFields of type Fields
     * @param argumentFields of type Fields
     * @param functor        of type Functor
     * @param threshold      of type int
     */
    public CompositeFunction( Fields groupingFields, Fields argumentFields, Functor functor, int threshold )
      {
      this( groupingFields, Fields.fields( argumentFields ), new Functor[]{functor}, threshold );
      }

    /**
     * Constructor CompositeFunction creates a new CompositeFunction instance.
     *
     * @param groupingFields of type Fields
     * @param argumentFields of type Fields[]
     * @param functors       of type Functor[]
     * @param threshold      of type int
     */
    public CompositeFunction( Fields groupingFields, Fields[] argumentFields, Functor[] functors, int threshold )
      {
      super( getFields( groupingFields, functors ) );
      this.groupingFields = groupingFields;
      this.argumentFields = argumentFields;
      this.functors = functors;
      this.threshold = threshold;

      functorFields = new Fields[functors.length];

      for( int i = 0; i < functors.length; i++ )
        functorFields[ i ] = functors[ i ].getDeclaredFields();
      }

    private static Fields getFields( Fields groupingFields, Functor[] functors )
      {
      Fields fields = groupingFields;

      for( int i = 0; i < functors.length; i++ )
        fields = fields.append( functors[ i ].getDeclaredFields() );

      return fields;
      }

    @Override
    public void prepare( final FlowProcess flowProcess, final OperationCall<LinkedHashMap<Tuple, Tuple[]>> operationCall )
      {
      operationCall.setContext( new LinkedHashMap<Tuple, Tuple[]>( threshold, 0.75f, true )
      {
      @Override
      protected boolean removeEldestEntry( Map.Entry<Tuple, Tuple[]> eldest )
        {
        boolean doRemove = size() > threshold;

        if( doRemove )
          completeFunctors( flowProcess, ( (FunctionCall) operationCall ).getOutputCollector(), eldest );

        return doRemove;
        }
      } );
      }

    @Override
    public void operate( FlowProcess flowProcess, FunctionCall<LinkedHashMap<Tuple, Tuple[]>> functionCall )
      {
      TupleEntry args = functionCall.getArguments();
      Tuple key = args.selectTuple( groupingFields );
      Tuple[] context = functionCall.getContext().get( key );

      if( context == null )
        {
        context = new Tuple[functors.length];
        functionCall.getContext().put( key, context );
        }

      for( int i = 0; i < functors.length; i++ )
        context[ i ] = functors[ i ].aggregate( flowProcess, args.selectEntry( argumentFields[ i ] ), context[ i ] );
      }

    @Override
    public void cleanup( FlowProcess flowProcess, OperationCall<LinkedHashMap<Tuple, Tuple[]>> operationCall )
      {
      // need to drain context
      TupleEntryCollector collector = ( (FunctionCall) operationCall ).getOutputCollector();

      for( Map.Entry<Tuple, Tuple[]> entry : operationCall.getContext().entrySet() )
        completeFunctors( flowProcess, collector, entry );

      operationCall.setContext( null );
      }

    private void completeFunctors( FlowProcess flowProcess, TupleEntryCollector outputCollector, Map.Entry<Tuple, Tuple[]> entry )
      {
      Tuple result = new Tuple( entry.getKey() );
      Tuple[] values = entry.getValue();

      for( int i = 0; i < functors.length; i++ )
        result.addAll( functors[ i ].complete( flowProcess, values[ i ] ) );

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

      if( threshold != that.threshold )
        return false;
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
      result = 31 * result + threshold;
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
   * @param name      of type String
   * @param threshold of type int
   */
  protected CompositeAggregator( String name, int threshold )
    {
    this.name = name;
    this.threshold = threshold;
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param argumentFields of type Fields
   * @param functor        of type Functor
   * @param aggregator     of type Aggregator
   */
  protected CompositeAggregator( Fields argumentFields, Functor functor, Aggregator aggregator )
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
  public CompositeAggregator( Pipe pipe, Fields groupingFields, CompositeAggregator... assemblies )
    {
    this( null, Pipe.pipes( pipe ), groupingFields, CompositeFunction.DEFAULT_THRESHOLD, assemblies );
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param threshold      of type int
   * @param assemblies     of type CompositeAggregator...
   */
  public CompositeAggregator( Pipe pipe, Fields groupingFields, int threshold, CompositeAggregator... assemblies )
    {
    this( null, Pipe.pipes( pipe ), groupingFields, threshold, assemblies );
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param assemblies     of type CompositeAggregator...
   */
  public CompositeAggregator( String name, Pipe[] pipes, Fields groupingFields, CompositeAggregator... assemblies )
    {
    this( name, pipes, groupingFields, CompositeFunction.DEFAULT_THRESHOLD, assemblies );
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param threshold      of type int
   * @param assemblies     of type CompositeAggregator...
   */
  public CompositeAggregator( String name, Pipe[] pipes, Fields groupingFields, int threshold, CompositeAggregator... assemblies )
    {
    this( name, threshold );

    List<Fields> arguments = new ArrayList<Fields>();
    List<Functor> functors = new ArrayList<Functor>();
    List<Aggregator> aggregators = new ArrayList<Aggregator>();

    for( int i = 0; i < assemblies.length; i++ )
      {
      CompositeAggregator assembly = assemblies[ i ];

      Collections.addAll( arguments, assembly.getArgumentFields() );
      Collections.addAll( functors, assembly.getFunctors() );
      Collections.addAll( aggregators, assembly.getAggregators() );
      }

    initialize( groupingFields, pipes, arguments.toArray( new Fields[0] ), functors.toArray( new Functor[0] ), aggregators.toArray( new Aggregator[0] ) );
    }

  protected CompositeAggregator( String name, Pipe[] pipes, Fields groupingFields, Fields argument, Functor functor, Aggregator aggregator, int threshold )
    {
    this( name, threshold );
    initialize( groupingFields, pipes, argument, functor, aggregator );
    }

  protected void initialize( Fields groupingFields, Pipe[] pipes, Fields argument, Functor functor, Aggregator aggregator )
    {
    initialize( groupingFields, pipes, Fields.fields( argument ),
      new Functor[]{functor},
      new Aggregator[]{aggregator} );
    }

  protected void initialize( Fields groupingFields, Pipe[] pipes, Fields[] argumentFields, Functor[] functors, Aggregator[] aggregators )
    {
    this.argumentFields = argumentFields;
    this.functors = functors;
    this.aggregators = aggregators;

    verify();

    Fields argumentSelector = Fields.merge( groupingFields, Fields.merge( argumentFields ) );

    Pipe[] functions = new Pipe[pipes.length];

    CompositeFunction function = new CompositeFunction( groupingFields, argumentFields, functors, threshold );

    for( int i = 0; i < functions.length; i++ )
      functions[ i ] = new Each( pipes[ i ], argumentSelector, function, Fields.RESULTS );

    Pipe pipe = new GroupBy( name, functions, groupingFields );

    for( int i = 0; i < aggregators.length; i++ )
      pipe = new Every( pipe, functors[ i ].getDeclaredFields(), aggregators[ i ], Fields.ALL );

    setTails( pipe );
    }

  /** Method verify should be overridden by sub-classes if any values must be tested before the calling constructor returns. */
  protected void verify()
    {

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
  }
