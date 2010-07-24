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

import cascading.operation.aggregator.First;
import cascading.operation.filter.FilterPartialDuplicates;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * Class Unique {@link SubAssembly} is used to filter all duplicates out of a tuple stream.
 * <p/>
 * Typically finding unique value in a tuple stream relies on a {@link GroupBy} and a {@link First()}
 * {@link cascading.operation.Aggregator} operation.
 * <p/>
 * This SubAssembly also uses the {@link FilterPartialDuplicates} {@link cascading.operation.Filter}
 * to remove as many observed duplicates before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying FilterPartialDuplicates how many values to cache for duplicate
 * comparison before dropping values from the LRU cache.
 */
public class Unique extends SubAssembly
  {

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields"})
  public Unique( Pipe pipe, Fields groupingFields )
    {
    this( null, pipe, groupingFields );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "threshold"})
  public Unique( Pipe pipe, Fields groupingFields, int threshold )
    {
    this( null, pipe, groupingFields, threshold );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields"})
  public Unique( String name, Pipe pipe, Fields groupingFields )
    {
    this( name, pipe, groupingFields, 10000 );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "threshold"})
  public Unique( String name, Pipe pipe, Fields groupingFields, int threshold )
    {
    pipe = new Each( pipe, groupingFields, new FilterPartialDuplicates( threshold ) );
    pipe = new GroupBy( name, pipe, groupingFields );
    pipe = new Every( pipe, Fields.ALL, new First(), Fields.RESULTS );

    setTails( pipe );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   */
  @ConstructorProperties({"pipes", "groupingFields"})
  public Unique( Pipe[] pipes, Fields groupingFields )
    {
    this( null, pipes, groupingFields, 10000 );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   */
  @ConstructorProperties({"name", "pipes", "groupingFields"})
  public Unique( String name, Pipe[] pipes, Fields groupingFields )
    {
    this( name, pipes, groupingFields, 10000 );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "threshold"})
  public Unique( String name, Pipe[] pipes, Fields groupingFields, int threshold )
    {
    Pipe[] filters = new Pipe[pipes.length];
    FilterPartialDuplicates partialDuplicates = new FilterPartialDuplicates( threshold );

    for( int i = 0; i < filters.length; i++ )
      filters[ i ] = new Each( pipes[ i ], groupingFields, partialDuplicates );

    Pipe pipe = new GroupBy( name, filters, groupingFields );
    pipe = new Every( pipe, Fields.ALL, new First(), Fields.RESULTS );

    setTails( pipe );
    }
  }
