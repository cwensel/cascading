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
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;

/**
 * Class Sum {@link cascading.pipe.SubAssembly} is used to sum values associated with duplicate keys in a tuple stream.
 * <p/>
 * Typically finding Sum value in a tuple stream relies on a {@link cascading.pipe.GroupBy} and a {@link cascading.operation.aggregator.Sum()}
 * {@link cascading.operation.Aggregator} operation.
 * <p/>
 * This SubAssembly also uses the {@link cascading.pipe.assembly.Sum.SumPartials} {@link cascading.operation.Function}
 * to count as many observed duplicates before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code threshold} value tells the underlying SumPartials functions how many values to cache for each
 * unique key before dropping values from the LRU cache.
 */
public class Sum extends SubAssembly
  {

  /**
   * Class SumPartials is a {@link cascading.operation.Function} that is used to sum observed duplicates from the tuple stream.
   * <p/>
   * Use this class typically in tandem with a {@link cascading.operation.aggregator.Sum}
   * {@link cascading.operation.Aggregator} in order to improve counting performance by removing as many values
   * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
   * <p/>
   * The {@code threshold} value is used to maintain a LRU of a constant size. If more than threshold Count values
   * are seen, the oldest cached values will be removed from the cache.
   *
   * @see Sum
   */
  public static class SumPartials extends BaseOperation<LinkedHashMap<Tuple, Double[]>> implements Function<LinkedHashMap<Tuple, Double[]>>
    {
    private Fields keyFields;
    private Fields valueField;
    private Class sumType;
    private int threshold = 10000;

    /** Constructor SumPartials creates a new SumPartials instance. */
    public SumPartials( Fields keyFields, Fields valueField, Fields sumField, Class sumType, int threshold )
      {
      super( keyFields.append( sumField ) );
      this.keyFields = keyFields;
      this.valueField = valueField;
      this.sumType = sumType;
      this.threshold = threshold;

      if( sumField.size() != 1 )
        throw new IllegalArgumentException( "sum fields may only have one field, got: " + sumField );

      if( valueField.size() != 1 )
        throw new IllegalArgumentException( "value fields may only have one field, got: " + valueField );
      }

    @Override
    public void prepare( FlowProcess flowProcess, final OperationCall<LinkedHashMap<Tuple, Double[]>> operationCall )
      {
      operationCall.setContext( new LinkedHashMap<Tuple, Double[]>( threshold, 0.75f, true )
      {
      @Override
      protected boolean removeEldestEntry( Map.Entry<Tuple, Double[]> eldest )
        {
        boolean doRemove = size() > threshold;

        if( doRemove )
          ( (FunctionCall) operationCall ).getOutputCollector().add( makeResult( eldest ) );

        return doRemove;
        }
      } );
      }

    private Tuple makeResult( Map.Entry<Tuple, Double[]> entry )
      {
      Tuple result = new Tuple( entry.getKey() );

      result.add( Tuples.coerce( entry.getValue()[ 0 ], sumType ) );

      return result;
      }

    @Override
    public void operate( FlowProcess flowProcess, FunctionCall<LinkedHashMap<Tuple, Double[]>> functionCall )
      {
      TupleEntry args = functionCall.getArguments();

      Tuple key = args.selectTuple( keyFields );
      Double value = args.getDouble( valueField );
      Double[] sum = functionCall.getContext().get( key );

      if( sum == null )
        functionCall.getContext().put( new Tuple( key ), new Double[]{value} );
      else
        sum[ 0 ] += value;
      }

    @Override
    public void cleanup( FlowProcess flowProcess, OperationCall<LinkedHashMap<Tuple, Double[]>> operationCall )
      {
      // need to drain context

      for( Map.Entry<Tuple, Double[]> entry : operationCall.getContext().entrySet() )
        ( (FunctionCall) operationCall ).getOutputCollector().add( makeResult( entry ) );

      operationCall.setContext( null );
      }

    @Override
    public boolean equals( Object object )
      {
      if( this == object )
        return true;
      if( !( object instanceof SumPartials ) )
        return false;
      if( !super.equals( object ) )
        return false;

      SumPartials that = (SumPartials) object;

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
   * Constructor Sum creates a new Sum instance.
   *
   * @param pipe       of type Pipe
   * @param keyFields  of type Fields
   * @param valueField of type Fields
   * @param sumField   of type Fields
   * @param sumType    of type Class
   */
  @ConstructorProperties({"pipe", "keyFields", "valueField", "sumField", "sumType"})
  public Sum( Pipe pipe, Fields keyFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( null, pipe, keyFields, valueField, sumField, sumType, 10000 );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param pipe       of type Pipe
   * @param keyFields  of type Fields
   * @param valueField of type Fields
   * @param sumField   of type Fields
   * @param sumType    of type Class
   * @param threshold  of type int
   */
  @ConstructorProperties({"pipe", "keyFields", "valueField", "sumField", "sumType", "threshold"})
  public Sum( Pipe pipe, Fields keyFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    this( null, pipe, keyFields, valueField, sumField, sumType, threshold );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param name       of type String
   * @param pipe       of type Pipe
   * @param keyFields  of type Fields
   * @param valueField of type Fields
   * @param sumField   of type Fields
   * @param sumType    of type Class
   */
  @ConstructorProperties({"name", "pipe", "keyFields", "valueField", "sumField", "sumType"})
  public Sum( String name, Pipe pipe, Fields keyFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( name, pipe, keyFields, valueField, sumField, sumType, 10000 );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param name       of type String
   * @param pipe       of type Pipe
   * @param keyFields  of type Fields
   * @param valueField of type Fields
   * @param sumField   of type Fields
   * @param sumType    of type Class
   * @param threshold  of type int
   */
  @ConstructorProperties({"name", "pipe", "keyFields", "valueField", "sumField", "sumType", "threshold"})
  public Sum( String name, Pipe pipe, Fields keyFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    if( !sumField.isDeclarator() || sumField.size() != 1 )
      throw new IllegalArgumentException( "sumField should declare only one field name" );

    pipe = new Each( pipe, keyFields.append( valueField ), new SumPartials( keyFields, valueField, sumField, sumType, threshold ), Fields.RESULTS );
    pipe = new GroupBy( name, pipe, keyFields );
    pipe = new Every( pipe, sumField, new cascading.operation.aggregator.Sum( sumField, sumType ), Fields.ALL );

    setTails( pipe );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param pipes      of type Pipe[]
   * @param keyFields  of type Fields
   * @param valueField of type Fields
   * @param sumField   of type Fields
   * @param sumType    of type Class
   */
  @ConstructorProperties({"name", "pipes", "keyFields", "valueField", "sumField", "sumType"})
  public Sum( Pipe[] pipes, Fields keyFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( null, pipes, keyFields, valueField, sumField, sumType, 10000 );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param pipes      of type Pipe[]
   * @param keyFields  of type Fields
   * @param valueField of type Fields
   * @param sumField   of type Fields
   * @param sumType    of type Class
   * @param threshold  of type int
   */
  @ConstructorProperties({"name", "pipes", "keyFields", "valueField", "sumField", "sumType", "threshold"})
  public Sum( Pipe[] pipes, Fields keyFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    this( null, pipes, keyFields, valueField, sumField, sumType, threshold );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param name       of type String
   * @param pipes      of type Pipe[]
   * @param keyFields  of type Fields
   * @param valueField of type Fields
   * @param sumField   of type Fields
   * @param sumType    of type Class
   */
  @ConstructorProperties({"name", "pipes", "keyFields", "valueField", "sumField", "sumType"})
  public Sum( String name, Pipe[] pipes, Fields keyFields, Fields valueField, Fields sumField, Class sumType )
    {
    this( name, pipes, keyFields, valueField, sumField, sumType, 10000 );
    }

  /**
   * Constructor Sum creates a new Sum instance.
   *
   * @param name       of type String
   * @param pipes      of type Pipe[]
   * @param keyFields  of type Fields
   * @param valueField of type Fields
   * @param sumField   of type Fields
   * @param sumType    of type Class
   * @param threshold  of type int
   */
  @ConstructorProperties({"name", "pipes", "keyFields", "valueField", "sumField", "sumType", "threshold"})
  public Sum( String name, Pipe[] pipes, Fields keyFields, Fields valueField, Fields sumField, Class sumType, int threshold )
    {
    if( !sumField.isDeclarator() || sumField.size() != 1 )
      throw new IllegalArgumentException( "sumField should declare only one field name" );

    Pipe[] functions = new Pipe[pipes.length];
    SumPartials partialDuplicates = new SumPartials( keyFields, valueField, sumField, sumType, threshold );

    for( int i = 0; i < functions.length; i++ )
      functions[ i ] = new Each( pipes[ i ], keyFields.append( valueField ), partialDuplicates, Fields.RESULTS );

    Pipe pipe = new GroupBy( name, functions, keyFields );
    pipe = new Every( pipe, sumField, new cascading.operation.aggregator.Sum( sumField, sumType ), Fields.ALL );

    setTails( pipe );
    }
  }
