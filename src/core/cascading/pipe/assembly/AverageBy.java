/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class AverageBy is used to average values associated with duplicate keys in a tuple stream.
 * <p/>
 * Typically finding the average value in a tuple stream relies on a {@link cascading.pipe.GroupBy} and a {@link cascading.operation.aggregator.Average}
 * {@link cascading.operation.Aggregator} operation.
 * <p/>
 * This SubAssembly uses the {@link cascading.pipe.assembly.AverageBy.AveragePartials} {@link cascading.pipe.assembly.AggregateBy.Functor}
 * and private {@link AverageFinal} Aggregator to count and sum as many observed duplicates before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying SumPartials functions how many values to cache for each
 * unique key before dropping values from the LRU cache.
 *
 * @see cascading.pipe.assembly.AggregateBy
 */
public class AverageBy extends AggregateBy
  {
  private static final Fields BIND_FIELDS = new Fields( AverageBy.class.getPackage().getName() + ".sum", AverageBy.class.getPackage().getName() + ".count" );

  /**
   * Class AveragePartials is a {@link cascading.pipe.assembly.AggregateBy.Functor} that is used to count and sum observed duplicates from the tuple stream.
   *
   * @see cascading.pipe.assembly.AverageBy
   */
  public static class AveragePartials implements Functor
    {
    /** Constructor SumPartials creates a new SumPartials instance. */
    public AveragePartials()
      {
      }

    @Override
    public Fields getDeclaredFields()
      {
      return BIND_FIELDS;
      }

    @Override
    public Tuple aggregate( FlowProcess flowProcess, TupleEntry args, Tuple context )
      {
      if( context == null )
        context = Tuple.size( 2 );

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
      double sum = 0.0D;
      long count = 0L;

      public Context reset()
        {
        sum = 0.0D;
        count = 0L;

        return this;
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

    public void start( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
      {
      if( aggregatorCall.getContext() != null )
        aggregatorCall.getContext().reset();
      else
        aggregatorCall.setContext( new Context() );
      }

    public void aggregate( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
      {
      Context context = aggregatorCall.getContext();
      TupleEntry arguments = aggregatorCall.getArguments();

      context.sum += arguments.getDouble( 0 );
      context.count += arguments.getLong( 1 );
      }

    public void complete( FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall )
      {
      aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
      }

    private Tuple getResult( AggregatorCall<Context> aggregatorCall )
      {
      Context context = aggregatorCall.getContext();

      return new Tuple( (Double) context.sum / context.count );
      }
    }

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
    super( valueField, new AveragePartials(), new AverageFinal( averageField ) );
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
    this( null, pipe, groupingFields, valueField, averageField, 10000 );
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
    this( name, pipe, groupingFields, valueField, averageField, 10000 );
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
    this( null, pipes, groupingFields, valueField, averageField, 10000 );
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
    this( name, pipes, groupingFields, valueField, averageField, 10000 );
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
    super( name, pipes, groupingFields, valueField, new AveragePartials(), new AverageFinal( averageField ), threshold );
    }
  }
