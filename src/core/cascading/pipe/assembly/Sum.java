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

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;

/**
 * Class Sum {@link CompositeAggregator} is used to sum values associated with duplicate keys in a tuple stream.
 * <p/>
 * Typically finding Sum value in a tuple stream relies on a {@link cascading.pipe.GroupBy} and a {@link cascading.operation.aggregator.Sum()}
 * {@link cascading.operation.Aggregator} operation.
 * <p/>
 * This SubAssembly also uses the {@link cascading.pipe.assembly.Sum.SumPartials} {@link Functor}
 * to count as many observed duplicates before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying SumPartials functions how many values to cache for each
 * unique key before dropping values from the LRU cache.
 *
 * @see cascading.pipe.assembly.CompositeAggregator
 */
public class Sum extends CompositeAggregator
  {
  /**
   * Class SumPartials is a {@link Functor} that is used to sum observed duplicates from the tuple stream.
   * <p/>
   * Use this class typically in tandem with a {@link cascading.operation.aggregator.Sum}
   * {@link cascading.operation.Aggregator} in order to improve counting performance by removing as many values
   * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
   *
   * @see Sum
   */
  public static class SumPartials implements Functor
    {
    private Fields declaredFields;
    private Class sumType;

    /** Constructor SumPartials creates a new SumPartials instance. */
    public SumPartials( Fields declaredFields, Class sumType )
      {
      this.declaredFields = declaredFields;
      this.sumType = sumType;

      if( declaredFields.size() != 1 )
        throw new IllegalArgumentException( "declared fields may only have one field, got: " + declaredFields );
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
        context = args.getTupleCopy();
      else
        context.set( 0, context.getDouble( 0 ) + args.getDouble( 0 ) );

      return context;
      }

    @Override
    public Tuple complete( FlowProcess flowProcess, Tuple context )
      {
      context.set( 0, Tuples.coerce( context.getDouble( 0 ), sumType ) );

      return context;
      }
    }

  /**
   * Constructor Sum creates a new Sum instance. Use this constructor when used with a {@link CompositeAggregator}
   * instance.
   *
   * @param valueField of type Fields
   * @param sumField   of type Fields
   * @param sumType    of type Class
   */
  @ConstructorProperties({"valueField", "sumField", "sumType"})
  public Sum( Fields valueField, Fields sumField, Class sumType )
    {
    super( valueField, new SumPartials( sumField, sumType ), new cascading.operation.aggregator.Sum( sumField, sumType ) );
    }

  //////////////

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "sumField", "sumType"})
  public Sum( Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( null, pipe, groupingFields, valueField, sumField, sumType, 10000 );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "valueField", "sumField", "sumType", "threshold"})
  public Sum( Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    this( null, pipe, groupingFields, valueField, sumField, sumType, threshold );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "sumField", "sumType"})
  public Sum( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( name, pipe, groupingFields, valueField, sumField, sumType, 10000 );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "valueField", "sumField", "sumType", "threshold"})
  public Sum( String name, Pipe pipe, Fields groupingFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, valueField, sumField, sumType, threshold );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "sumField", "sumType"})
  public Sum( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( null, pipes, groupingFields, valueField, sumField, sumType, 10000 );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "sumField", "sumType", "threshold"})
  public Sum( Pipe[] pipes, Fields groupingFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    this( null, pipes, groupingFields, valueField, sumField, sumType, threshold );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "sumField", "sumType"})
  public Sum( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( name, pipes, groupingFields, valueField, sumField, sumType, 10000 );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param valueField     of type Fields
   * @param sumField       of type Fields
   * @param sumType        of type Class
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "valueField", "sumField", "sumType", "threshold"})
  public Sum( String name, Pipe[] pipes, Fields groupingFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    super( name, pipes, groupingFields, valueField, new SumPartials( sumField, sumType ), new cascading.operation.aggregator.Sum( sumField, sumType ), threshold );
    }
  }
