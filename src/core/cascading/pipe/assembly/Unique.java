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
import java.util.LinkedHashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.operation.aggregator.First;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class Unique {@link SubAssembly} is used to filter all duplicates out of a tuple stream.
 * <p/>
 * Typically finding unique value in a tuple stream relies on a {@link GroupBy} and a {@link First}
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
   * Class FilterPartialDuplicates is a {@link cascading.operation.Filter} that is used to remove observed duplicates from the tuple stream.
   * <p/>
   * Use this class typically in tandem with a {@link cascading.operation.aggregator.First}
   * {@link cascading.operation.Aggregator} in order to improve de-duping performance by removing as many values
   * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
   * <p/>
   * The {@code threshold} value is used to maintain a LRU of a constant size. If more than threshold unique values
   * are seen, the oldest cached values will be removed from the cache.
   *
   * @see Unique
   */
  public static class FilterPartialDuplicates extends BaseOperation<LinkedHashMap<Tuple, Object>> implements Filter<LinkedHashMap<Tuple, Object>>
    {
    private int threshold = 10000;

    /** Constructor FilterPartialDuplicates creates a new FilterPartialDuplicates instance. */
    public FilterPartialDuplicates()
      {
      }

    /**
     * Constructor FilterPartialDuplicates creates a new FilterPartialDuplicates instance.
     *
     * @param threshold of type int
     */
    @ConstructorProperties({"threshold"})
    public FilterPartialDuplicates( int threshold )
      {
      this.threshold = threshold;
      }

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<LinkedHashMap<Tuple, Object>> operationCall )
      {
      operationCall.setContext( new LinkedHashMap<Tuple, Object>( threshold, 0.75f, true )
      {
      @Override
      protected boolean removeEldestEntry( Map.Entry eldest )
        {
        return size() > threshold;
        }
      } );
      }

    @Override
    public boolean isRemove( FlowProcess flowProcess, FilterCall<LinkedHashMap<Tuple, Object>> filterCall )
      {
      // we assume its more painful to create lots of tuple copies vs comparisons
      Tuple args = filterCall.getArguments().getTuple();

      if( filterCall.getContext().containsKey( args ) )
        return true;

      filterCall.getContext().put( filterCall.getArguments().getTupleCopy(), null );

      return false;
      }

    @Override
    public void cleanup( FlowProcess flowProcess, OperationCall<LinkedHashMap<Tuple, Object>> operationCall )
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

      if( threshold != that.threshold )
        return false;

      return true;
      }

    @Override
    public int hashCode()
      {
      int result = super.hashCode();
      result = 31 * result + threshold;
      return result;
      }
    }

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
    this( name, Pipe.pipes( pipe ), groupingFields, threshold );
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
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipes", "groupingFields", "threshold"})
  public Unique( Pipe[] pipes, Fields groupingFields, int threshold )
    {
    this( null, pipes, groupingFields, threshold );
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
    Pipe[] filters = new Pipe[ pipes.length ];
    FilterPartialDuplicates partialDuplicates = new FilterPartialDuplicates( threshold );

    for( int i = 0; i < filters.length; i++ )
      filters[ i ] = new Each( pipes[ i ], groupingFields, partialDuplicates );

    Pipe pipe = new GroupBy( name, filters, groupingFields );
    pipe = new Every( pipe, Fields.ALL, new First(), Fields.RESULTS );

    setTails( pipe );
    }
  }
