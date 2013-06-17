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
import java.util.LinkedHashMap;
import java.util.Map;

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
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;

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
 * The {@code threshold} value tells the underlying FilterPartialDuplicates how many values to cache for duplicate
 * comparison before dropping values from the LRU cache.
 */
public class Unique extends SubAssembly
  {
  public enum Include
    {
      ALL,
      NO_NULLS
    }

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
    private Include include = Include.ALL;

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

    /**
     * Constructor FilterPartialDuplicates creates a new FilterPartialDuplicates instance.
     *
     * @param threshold of type int
     */
    @ConstructorProperties({"include", "threshold"})
    public FilterPartialDuplicates( Include include, int threshold )
      {
      this.threshold = threshold;
      this.include = include == null ? this.include : include;
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
   * @param threshold    of type int
   */
  @ConstructorProperties({"pipe", "uniqueFields", "threshold"})
  public Unique( Pipe pipe, Fields uniqueFields, int threshold )
    {
    this( null, pipe, uniqueFields, threshold );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipe         of type Pipe
   * @param uniqueFields of type Fields
   * @param include      of type Include
   * @param threshold    of type int
   */
  @ConstructorProperties({"pipe", "uniqueFields", "include", "threshold"})
  public Unique( Pipe pipe, Fields uniqueFields, Include include, int threshold )
    {
    this( null, pipe, uniqueFields, include, threshold );
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
    this( name, pipe, uniqueFields, 10000 );
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
    this( name, pipe, uniqueFields, include, 10000 );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name         of type String
   * @param pipe         of type Pipe
   * @param uniqueFields of type Fields
   * @param threshold    of type int
   */
  @ConstructorProperties({"name", "pipe", "uniqueFields", "threshold"})
  public Unique( String name, Pipe pipe, Fields uniqueFields, int threshold )
    {
    this( name, Pipe.pipes( pipe ), uniqueFields, threshold );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name         of type String
   * @param pipe         of type Pipe
   * @param uniqueFields of type Fields
   * @param include      of type Include
   * @param threshold    of type int
   */
  @ConstructorProperties({"name", "pipe", "uniqueFields", "include", "threshold"})
  public Unique( String name, Pipe pipe, Fields uniqueFields, Include include, int threshold )
    {
    this( name, Pipe.pipes( pipe ), uniqueFields, include, threshold );
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
    this( null, pipes, uniqueFields, 10000 );
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
    this( null, pipes, uniqueFields, include, 10000 );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipes        of type Pipe[]
   * @param uniqueFields of type Fields
   * @param threshold    of type int
   */
  @ConstructorProperties({"pipes", "uniqueFields", "threshold"})
  public Unique( Pipe[] pipes, Fields uniqueFields, int threshold )
    {
    this( null, pipes, uniqueFields, threshold );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param pipes        of type Pipe[]
   * @param uniqueFields of type Fields
   * @param include      of type Include
   * @param threshold    of type int
   */
  @ConstructorProperties({"pipes", "uniqueFields", "threshold"})
  public Unique( Pipe[] pipes, Fields uniqueFields, Include include, int threshold )
    {
    this( null, pipes, uniqueFields, include, threshold );
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
    this( name, pipes, uniqueFields, 10000 );
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
    this( name, pipes, uniqueFields, include, 10000 );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name         of type String
   * @param pipes        of type Pipe[]
   * @param uniqueFields of type Fields
   * @param threshold    of type int
   */
  @ConstructorProperties({"name", "pipes", "uniqueFields", "threshold"})
  public Unique( String name, Pipe[] pipes, Fields uniqueFields, int threshold )
    {
    this( name, pipes, uniqueFields, null, threshold );
    }

  /**
   * Constructor Unique creates a new Unique instance.
   *
   * @param name         of type String
   * @param pipes        of type Pipe[]
   * @param uniqueFields of type Fields
   * @param threshold    of type int
   */
  @ConstructorProperties({"name", "pipes", "uniqueFields", "include", "threshold"})
  public Unique( String name, Pipe[] pipes, Fields uniqueFields, Include include, int threshold )
    {
    super( pipes );

    Pipe[] filters = new Pipe[ pipes.length ];
    FilterPartialDuplicates partialDuplicates = new FilterPartialDuplicates( include, threshold );

    for( int i = 0; i < filters.length; i++ )
      filters[ i ] = new Each( pipes[ i ], uniqueFields, partialDuplicates );

    Pipe pipe = new GroupBy( name, filters, uniqueFields );
    pipe = new Every( pipe, Fields.ALL, new FirstNBuffer(), Fields.RESULTS );

    setTails( pipe );
    }
  }
