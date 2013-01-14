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
import java.lang.reflect.Type;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;

/**
 * Class AverageBy is used to average values associated with duplicate keys in a tuple stream.
 * <p/>
 * Typically finding the average value in a tuple stream relies on a {@link cascading.pipe.GroupBy} and a {@link cascading.operation.aggregator.Average}
 * {@link cascading.operation.Aggregator} operation.
 * <p/>
 * If the given {@code averageFields} has an associated type, this type will be used to coerce the resulting average value,
 * otherwise the result will be a {@link Double}.
 * <p/>
 * If {@code include} is {@link Include#NO_NULLS}, {@code null} values will not be included in the average (converted to zero).
 * By default (and for backwards compatibility) {@code null} values are included, {@link Include#ALL}.
 * <p/>
 * This SubAssembly uses the {@link cascading.pipe.assembly.AverageBy.AveragePartials} {@link cascading.pipe.assembly.AggregateBy.Functor}
 * and private {@link AverageFinal} Aggregator to count and sum as many field values before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying AveragePartials functions how many unique key sums and counts to accumulate
 * in the LRU cache, before emitting the least recently used entry.
 *
 * @see cascading.pipe.assembly.AggregateBy
 */
public class AverageBy extends AggregateBy
  {
  /** DEFAULT_THRESHOLD */
  public static final int DEFAULT_THRESHOLD = 10000;

  public enum Include
    {
      ALL,
      NO_NULLS
    }

  /**
   * Class AveragePartials is a {@link cascading.pipe.assembly.AggregateBy.Functor} that is used to count and sum observed duplicates from the tuple stream.
   *
   * @see cascading.pipe.assembly.AverageBy
   */
  public static class AveragePartials implements Functor
    {
    private final Fields declaredFields;
    private final Include include;

    /**
     * Constructor AveragePartials creates a new AveragePartials instance.
     *
     * @param declaredFields of type Fields
     */
    public AveragePartials( Fields declaredFields )
      {
      this.declaredFields = declaredFields;
      this.include = Include.ALL;
      }

    public AveragePartials( Fields declaredFields, Include include )
      {
      this.declaredFields = declaredFields;
      this.include = include;
      }

    @Override
    public Fields getDeclaredFields()
      {
      return new Fields( AverageBy.class.getPackage().getName() + "." + declaredFields.get( 0 ) + ".sum", AverageBy.class.getPackage().getName() + "." + declaredFields.get( 0 ) + ".count" );
      }

    @Override
    public Tuple aggregate( FlowProcess flowProcess, TupleEntry args, Tuple context )
      {
      if( context == null )
        context = Tuple.size( 2 );

      if( include == Include.NO_NULLS && args.getObject( 0 ) == null )
        return context;

      context.set( 0, context.getDouble( 0 ) + args.getDouble( 0 ) );
      context.set( 1, context.getLong( 1 ) + 1 );

      return context;
      }

    @Override
    public Tuple complete( FlowProcess flowProcess, Tuple context )
      {
      return context;
      }
    }

  /**
   * Class AverageFinal is used to finalize the average operation on the Reduce side of the process. It must be used
   * in tandem with a {@link AveragePartials} Functor.
   */
  public static class AverageFinal extends BaseOperation<AverageFinal.Context> implements Aggregator<AverageFinal.Context>
    {
    /** Class Context is used to hold intermediate values. */
    protected static class Context
      {
      long nulls = 0L;
      double sum = 0.0D;
      long count = 0L;
      Type type = Double.class;
      CoercibleType canonical;

      Tuple tuple = Tuple.size( 1 );

      public Context( Fields fieldDeclaration )
        {
        if( fieldDeclaration.hasTypes() )
          this.type = fieldDeclaration.getType( 0 );

        this.canonical = Coercions.coercibleTypeFor( this.type );
        }

      public Context reset()
        {
        nulls = 0L;
        sum = 0.0D;
        count = 0L;
        tuple.set( 0, null );

        return this;
        }

      public Tuple result()
        {
        // we only saw null from upstream, so return null
        if( count == 0 && nulls != 0 )
          return tuple;

        tuple.set( 0, canonical.canonical( sum / count ) );

        return tuple;
        }
      }

    /**
     * Constructs a new instance that returns the average of the values encountered in the given fieldDeclaration field name.
     *
     * @param fieldDeclaration of type Fields
     */
    public AverageFinal( Fields fieldDeclaration )
      {
      super( 2, fieldDeclaration );

      if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1 )
        throw new IllegalArgumentException( "fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size() );
      }

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall )
      {
      operationCall.setContext( new Context( getFieldDeclaration() ) );
      }

    @Override
    public void start( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
      {
      aggregatorCall.getContext().reset();
      }

    @Override
    public void aggregate( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
      {
      Context context = aggregatorCall.getContext();
      TupleEntry arguments = aggregatorCall.getArguments();

      if( arguments.getObject( 0 ) == null )
        {
        context.nulls++;
        return;
        }

      context.sum += arguments.getDouble( 0 );
      context.count += arguments.getLong( 1 );
      }

    @Override
    public void complete( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
      {
      aggregatorCall.getOutputCollector().add( aggregatorCall.getContext().result() );
      }
    }

  /////////

  /**
   * Constructor AverageBy creates a new AverageBy instance. Use this constructor when used with a {@link cascading.pipe.assembly.AggregateBy}
   * instance.
   *
   * @param valueField   of type Fields
   * @param averageField of type Fields
   */
  @ConstructorProperties({"valueField", "averageField"})
  public AverageBy( Fields valueField, Fields averageField )
    {
    super( valueField, new AveragePartials( averageField ), new AverageFinal( averageField ) );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance. Use this constructor when used with a {@link cascading.pipe.assembly.AggregateBy}
   * instance.
   *
   * @param valueField   of type Fields
   * @param averageField of type Fields
   * @param include      of type boolean
   */
  @ConstructorProperties({"valueField", "averageField", "include"})
  public AverageBy( Fields valueField, Fields averageField, Include include )
    {
    super( valueField, new AveragePartials( averageField, include ), new AverageFinal( averageField ) );
    }

  //////////////

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "averageField"})
  public AverageBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField )
    {
    this( null, pipe, groupingFields, valueField, averageField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "averageField", "threshold"})
  public AverageBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField, int threshold )
    {
    this( null, pipe, groupingFields, valueField, averageField, threshold );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "averageField"})
  public AverageBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField )
    {
    this( name, pipe, groupingFields, valueField, averageField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "averageField", "threshold"})
  public AverageBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField, int threshold )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, valueField, averageField, threshold );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "averageField"})
  public AverageBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields averageField )
    {
    this( null, pipes, groupingFields, valueField, averageField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "averageField", "threshold"})
  public AverageBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields averageField, int threshold )
    {
    this( null, pipes, groupingFields, valueField, averageField, threshold );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "averageField"})
  public AverageBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields averageField )
    {
    this( name, pipes, groupingFields, valueField, averageField, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "averageField", "threshold"})
  public AverageBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields averageField, int threshold )
    {
    super( name, pipes, groupingFields, valueField, new AveragePartials( averageField ), new AverageFinal( averageField ), threshold );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param include        of type boolean
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "averageField", "include"})
  public AverageBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField, Include include )
    {
    this( null, pipe, groupingFields, valueField, averageField, include, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param include        of type boolean
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "averageField", "include", "threshold"})
  public AverageBy( Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField, Include include, int threshold )
    {
    this( null, pipe, groupingFields, valueField, averageField, include, threshold );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param include        of type boolean
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "averageField", "include"})
  public AverageBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField, Include include )
    {
    this( name, pipe, groupingFields, valueField, averageField, include, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param include        of type boolean
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "averageField", "include", "threshold"})
  public AverageBy( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields averageField, Include include, int threshold )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, valueField, averageField, include, threshold );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param include        of type boolean
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "averageField", "include"})
  public AverageBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields averageField, Include include )
    {
    this( null, pipes, groupingFields, valueField, averageField, include, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param include        of type boolean
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "averageField", "include", "threshold"})
  public AverageBy( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields averageField, Include include, int threshold )
    {
    this( null, pipes, groupingFields, valueField, averageField, include, threshold );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param include        of type boolean
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "averageField", "include"})
  public AverageBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields averageField, Include include )
    {
    this( name, pipes, groupingFields, valueField, averageField, include, DEFAULT_THRESHOLD );
    }

  /**
   * Constructor AverageBy creates a new AverageBy instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param averageField   of type Fields
   * @param include        of type boolean
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "averageField", "include", "threshold"})
  public AverageBy( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields averageField, Include include, int threshold )
    {
    super( name, pipes, groupingFields, valueField, new AveragePartials( averageField, include ), new AverageFinal( averageField ), threshold );
    }
  }
