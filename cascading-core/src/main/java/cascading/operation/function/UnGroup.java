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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger( UnGroup.class );

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
    if( valueSelectors == null || valueSelectors.length == 1 )
      throw new IllegalArgumentException( "value selectors may not be empty" );

    int size = valueSelectors[ 0 ].size();

    for( int i = 1; i < valueSelectors.length; i++ )
      {
      if( valueSelectors[ 0 ].size() != valueSelectors[ i ].size() )
        throw new IllegalArgumentException( "all value selectors must be the same size" );

      size = valueSelectors[ i ].size();
      }

    this.numArgs = groupSelector.size() + size * valueSelectors.length;
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

    if( valueSelectors == null || valueSelectors.length == 1 )
      throw new IllegalArgumentException( "value selectors may not be empty" );

    numArgs = groupSelector.size();
    int selectorSize = -1;

    for( Fields resultFieldSelector : valueSelectors )
      {
      numArgs += resultFieldSelector.size();
      int fieldSize = groupSelector.size() + resultFieldSelector.size();

      if( selectorSize != -1 && selectorSize != resultFieldSelector.size() )
        throw new IllegalArgumentException( "all value selectors must be the same size, and this size plus group selector size must equal the declared field size" );

      selectorSize = resultFieldSelector.size();

      if( fieldDeclaration.size() != fieldSize )
        throw new IllegalArgumentException( "all value selectors must be the same size, and this size plus group selector size must equal the declared field size" );
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

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    if( resultFieldSelectors != null )
      useResultSelectors( functionCall.getArguments(), functionCall.getOutputCollector() );
    else
      useSize( functionCall.getArguments(), functionCall.getOutputCollector() );
    }

  private void useSize( TupleEntry input, TupleEntryCollector outputCollector )
    {
    LOG.debug( "using size: {}", size );

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
    LOG.debug( "using result selectors: {}", resultFieldSelectors.length );

    for( Fields resultFieldSelector : resultFieldSelectors )
      {
      Tuple group = input.selectTupleCopy( groupFieldSelector ); // need a mutable copy

      input.selectInto( resultFieldSelector, group );

      outputCollector.add( group );
      }
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof UnGroup ) )
      return false;
    if( !super.equals( object ) )
      return false;

    UnGroup unGroup = (UnGroup) object;

    if( size != unGroup.size )
      return false;
    if( groupFieldSelector != null ? !groupFieldSelector.equals( unGroup.groupFieldSelector ) : unGroup.groupFieldSelector != null )
      return false;
    if( !Arrays.equals( resultFieldSelectors, unGroup.resultFieldSelectors ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( groupFieldSelector != null ? groupFieldSelector.hashCode() : 0 );
    result = 31 * result + ( resultFieldSelectors != null ? Arrays.hashCode( resultFieldSelectors ) : 0 );
    result = 31 * result + size;
    return result;
    }
  }
