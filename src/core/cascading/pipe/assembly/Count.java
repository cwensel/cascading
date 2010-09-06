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
import java.util.LinkedHashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class Count {@link cascading.pipe.SubAssembly} is used to count duplicates in a tuple stream.
 * <p/>
 * Typically finding Count value in a tuple stream relies on a {@link cascading.pipe.GroupBy} and a {@link cascading.operation.aggregator.Count()}
 * {@link cascading.operation.Aggregator} operation.
 * <p/>
 * This SubAssembly also uses the {@link cascading.pipe.assembly.Count.CountPartials} {@link cascading.operation.Function}
 * to count as many observed duplicates before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying CountPartials functions how many values to cache for each
 * unique key before dropping values from the LRU cache.
 */
public class Count extends SubAssembly
  {

  /**
   * Class CountPartials is a {@link cascading.operation.Function} that is used to count observed duplicates from the tuple stream.
   * <p/>
   * Use this class typically in tandem with a {@link cascading.operation.aggregator.Sum}
   * {@link cascading.operation.Aggregator} in order to improve counting performance by removing as many values
   * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
   * <p/>
   * The {@code threshold} value is used to maintain a LRU of a constant size. If more than threshold Count values
   * are seen, the oldest cached values will be removed from the cache.
   *
   * @see cascading.pipe.assembly.Count
   */
  public static class CountPartials extends BaseOperation<LinkedHashMap<Tuple, Long[]>> implements Function<LinkedHashMap<Tuple, Long[]>>
    {
    private int threshold = 10000;

    /**
     * Constructor CountPartials creates a new CountPartials instance.
     *
     * @param declaredFields of type Fields
     * @param threshold      of type int
     */
    @ConstructorProperties({"threshold"})
    public CountPartials( Fields declaredFields, int threshold )
      {
      super( declaredFields );
      this.threshold = threshold;
      }

    @Override
    public void prepare( FlowProcess flowProcess, final OperationCall<LinkedHashMap<Tuple, Long[]>> operationCall )
      {
      operationCall.setContext( new LinkedHashMap<Tuple, Long[]>( threshold, 0.75f, true )
      {
      @Override
      protected boolean removeEldestEntry( Map.Entry<Tuple, Long[]> eldest )
        {
        boolean doRemove = size() > threshold;

        if( doRemove )
          ( (FunctionCall) operationCall ).getOutputCollector().add( makeResult( eldest ) );

        return doRemove;
        }
      } );
      }

    private Tuple makeResult( Map.Entry<Tuple, Long[]> entry )
      {
      Tuple result = new Tuple( entry.getKey() );

      result.add( entry.getValue()[ 0 ] );

      return result;
      }

    @Override
    public void operate( FlowProcess flowProcess, FunctionCall<LinkedHashMap<Tuple, Long[]>> functionCall )
      {
      // we assume its more painful to create lots of tuple copies vs comparisons
      Tuple args = functionCall.getArguments().getTuple();

      Long[] count = functionCall.getContext().get( args );

      if( count == null )
        functionCall.getContext().put( functionCall.getArguments().getTupleCopy(), new Long[]{1L} );
      else
        count[ 0 ]++;
      }

    @Override
    public void cleanup( FlowProcess flowProcess, OperationCall<LinkedHashMap<Tuple, Long[]>> operationCall )
      {
      // need to drain context

      for( Map.Entry<Tuple, Long[]> entry : operationCall.getContext().entrySet() )
        ( (FunctionCall) operationCall ).getOutputCollector().add( makeResult( entry ) );

      operationCall.setContext( null );
      }

    @Override
    public boolean equals( Object object )
      {
      if( this == object )
        return true;
      if( !( object instanceof CountPartials ) )
        return false;
      if( !super.equals( object ) )
        return false;

      CountPartials that = (CountPartials) object;

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
   * Constructor Count creates a new Count instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   */
  @ConstructorProperties({"pipe", "groupingFields", "countField"})
  public Count( Pipe pipe, Fields groupingFields, Fields countField )
    {
    this( null, pipe, groupingFields, countField );
    }

  /**
   * Constructor Count creates a new Count instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     fo type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipe", "groupingFields", "countField", "threshold"})
  public Count( Pipe pipe, Fields groupingFields, Fields countField, int threshold )
    {
    this( null, pipe, groupingFields, countField, threshold );
    }

  /**
   * Constructor Count creates a new Count instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "countField"})
  public Count( String name, Pipe pipe, Fields groupingFields, Fields countField )
    {
    this( name, pipe, groupingFields, countField, 10000 );
    }

  /**
   * Constructor Count creates a new Count instance.
   *
   * @param name           of type String
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipe", "groupingFields", "countField", "threshold"})
  public Count( String name, Pipe pipe, Fields groupingFields, Fields countField, int threshold )
    {
    if( !countField.isDeclarator() || countField.size() != 1 )
      throw new IllegalArgumentException( "countField should declare only one field name" );

    pipe = new Each( pipe, groupingFields, new CountPartials( groupingFields.append( countField ), threshold ), Fields.RESULTS );
    pipe = new GroupBy( name, pipe, groupingFields );
    pipe = new Every( pipe, countField, new Sum( countField, Long.TYPE ), Fields.ALL );

    setTails( pipe );
    }

  /**
   * Constructor Count creates a new Count instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   */
  @ConstructorProperties({"pipes", "groupingFields", "countField"})
  public Count( Pipe[] pipes, Fields groupingFields, Fields countField )
    {
    this( null, pipes, groupingFields, countField, 10000 );
    }

  /**
   * Constructor Count creates a new Count instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"pipes", "groupingFields", "countField", "threshold"})
  public Count( Pipe[] pipes, Fields groupingFields, Fields countField, int threshold )
    {
    this( null, pipes, groupingFields, countField, threshold );
    }

  /**
   * Constructor Count creates a new Count instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "countField"})
  public Count( String name, Pipe[] pipes, Fields groupingFields, Fields countField )
    {
    this( name, pipes, groupingFields, countField, 10000 );
    }

  /**
   * Constructor Count creates a new Count instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param countField     of type Fields
   * @param threshold      of type int
   */
  @ConstructorProperties({"name", "pipes", "groupingFields", "countField", "threshold"})
  public Count( String name, Pipe[] pipes, Fields groupingFields, Fields countField, int threshold )
    {
    if( !countField.isDeclarator() || countField.size() != 1 )
      throw new IllegalArgumentException( "countField should declare only one field name" );

    Pipe[] functions = new Pipe[pipes.length];
    CountPartials partialDuplicates = new CountPartials( groupingFields.append( countField ), threshold );

    for( int i = 0; i < functions.length; i++ )
      functions[ i ] = new Each( pipes[ i ], groupingFields, partialDuplicates, Fields.RESULTS );

    Pipe pipe = new GroupBy( name, functions, groupingFields );
    pipe = new Every( pipe, countField, new Sum( countField, Long.TYPE ), Fields.ALL );

    setTails( pipe );
    }
  }
