/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.function;

import java.beans.ConstructorProperties;
import java.util.Arrays;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.log4j.Logger;

/**
 * Class UnGroup is a {@link Function} that will 'un-group' data from a given dataset.
 * <p/>
 * That is, for the given field positions, this function will emit a new Tuple for every value. For example:
 * <p/>
 * <pre>
 * A, x, y
 * B, x, z
 * C, y, z
 * </pre>
 * <p/>
 * to:
 * <p/>
 * <pre>
 * A, x
 * A, y
 * B, x
 * B, z
 * C, y
 * C, z
 * </pre>
 */
public class UnGroup extends BaseOperation implements Function
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( UnGroup.class );

  /** Field groupFieldSelector */
  private Fields groupFieldSelector;
  /** Field resultFieldSelectors */
  private Fields[] resultFieldSelectors;
  /** Field size */
  private int size = 1;

  /**
   * Constructor UnGroup creates a new UnGroup instance.
   *
   * @param groupSelector  of type Fields
   * @param valueSelectors of type Fields[]
   */
  @ConstructorProperties({"groupSelector", "valueSelectors"})
  public UnGroup( Fields groupSelector, Fields[] valueSelectors )
    {
    int size = 0;

    for( Fields resultFieldSelector : valueSelectors )
      {
      size = resultFieldSelector.size();
      numArgs = groupSelector.size() + size;

      if( fieldDeclaration.size() != numArgs )
        throw new IllegalArgumentException( "all field selectors must be the same size, and this size plus group selector size must equal the declared field size" );
      }

    this.groupFieldSelector = groupSelector;
    this.resultFieldSelectors = Arrays.copyOf( valueSelectors, valueSelectors.length );
    this.fieldDeclaration = Fields.size( groupSelector.size() + size );
    }

  /**
   * Constructor UnGroup creates a new UnGroup instance.
   *
   * @param fieldDeclaration of type Fields
   * @param groupSelector    of type Fields
   * @param valueSelectors   of type Fields[]
   */
  @ConstructorProperties({"fieldDeclaration", "groupSelector", "valueSelectors"})
  public UnGroup( Fields fieldDeclaration, Fields groupSelector, Fields[] valueSelectors )
    {
    super( fieldDeclaration );

    numArgs = groupSelector.size();
    int selectorSize = -1;

    for( Fields resultFieldSelector : valueSelectors )
      {
      numArgs += resultFieldSelector.size();
      int fieldSize = groupSelector.size() + resultFieldSelector.size();

      if( selectorSize != -1 && selectorSize != resultFieldSelector.size() )
        throw new IllegalArgumentException( "all field selectors must be the same size, and this size plus group selector size must equal the declared field size" );

      selectorSize = resultFieldSelector.size();

      if( fieldDeclaration.size() != fieldSize )
        throw new IllegalArgumentException( "all field selectors must be the same size, and this size plus group selector size must equal the declared field size" );
      }

    this.groupFieldSelector = groupSelector;
    this.resultFieldSelectors = Arrays.copyOf( valueSelectors, valueSelectors.length );
    }

  /**
   * Constructor UnGroup creates a new UnGroup instance. Where the numValues argument specifies the number
   * of values to include.
   *
   * @param fieldDeclaration of type Fields
   * @param groupSelector    of type Fields
   * @param numValues        of type int
   */
  @ConstructorProperties({"fieldDeclaration", "groupSelector", "numValues"})
  public UnGroup( Fields fieldDeclaration, Fields groupSelector, int numValues )
    {
    super( fieldDeclaration );
    this.groupFieldSelector = groupSelector;
    this.size = numValues;
    }

  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    if( resultFieldSelectors != null )
      useResultSelectors( functionCall.getArguments(), functionCall.getOutputCollector() );
    else
      useSize( functionCall.getArguments(), functionCall.getOutputCollector() );
    }

  private void useSize( TupleEntry input, TupleEntryCollector outputCollector )
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "using size: " + size );

    Tuple tuple = new Tuple( input.getTuple() ); // make clone
    Tuple group = tuple.remove( input.getFields(), groupFieldSelector );

    for( int i = 0; i < tuple.size(); i = i + size )
      {
      Tuple result = new Tuple( group );
      result.addAll( tuple.get( Fields.offsetSelector( size, i ).getPos() ) );

      outputCollector.add( result );
      }
    }

  private void useResultSelectors( TupleEntry input, TupleEntryCollector outputCollector )
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "using result selectors: " + resultFieldSelectors.length );

    for( Fields resultFieldSelector : resultFieldSelectors )
      {
      Tuple group = input.selectTuple( groupFieldSelector );

      group.addAll( input.selectTuple( resultFieldSelector ) );

      outputCollector.add( group );
      }
    }
  }
