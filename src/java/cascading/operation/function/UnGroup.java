/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;
import org.apache.log4j.Logger;

/**
 * For the given field positions, emit a new Tuple for every value.
 * Used for:
 * <p/>
 * A, x, y
 * B, x, z
 * C, y, z
 * <p/>
 * to:
 * <p/>
 * A, x
 * A, y
 * B, x
 * B, z
 * C, y
 * C, z
 */
public class UnGroup extends BaseOperation implements Function
  {
  private static final Logger LOG = Logger.getLogger( UnGroup.class );

  private Fields groupFieldSelector;
  private Fields[] resultFieldSelectors;
  private int size = 1;

  public UnGroup( Fields groupFieldSelector, Fields[] resultFieldSelectors )
    {
    int size = 0;

    for( Fields resultFieldSelector : resultFieldSelectors )
      {
      size = resultFieldSelector.size();
      numArgs = groupFieldSelector.size() + size;

      if( fieldDeclaration.size() != numArgs )
        throw new IllegalArgumentException( "all field selectors must be the same size, and this size plus group selector size must equal the declared field size" );
      }

    this.groupFieldSelector = groupFieldSelector;
    this.resultFieldSelectors = resultFieldSelectors;
    this.fieldDeclaration = Fields.size( groupFieldSelector.size() + size );
    }

  public UnGroup( Fields fieldDeclaration, Fields groupFieldSelector, Fields[] resultFieldSelectors )
    {
    super( fieldDeclaration );

    for( Fields resultFieldSelector : resultFieldSelectors )
      {
      numArgs = groupFieldSelector.size() + resultFieldSelector.size();

      if( fieldDeclaration.size() != numArgs )
        throw new IllegalArgumentException( "all field selectors must be the same size, and this size plus group selector size must equal the declared field size" );
      }

    this.groupFieldSelector = groupFieldSelector;
    this.resultFieldSelectors = resultFieldSelectors;
    }

  public UnGroup( Fields fieldDeclaration, Fields groupFieldSelector, int size )
    {
    super( fieldDeclaration );
    this.groupFieldSelector = groupFieldSelector;
    this.size = size;
    }

  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    if( resultFieldSelectors != null )
      useResultSelectors( functionCall.getArguments(), functionCall.getOutputCollector() );
    else
      useSize( functionCall.getArguments(), functionCall.getOutputCollector() );
    }

  private void useSize( TupleEntry input, TupleCollector outputCollector )
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

  private void useResultSelectors( TupleEntry input, TupleCollector outputCollector )
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
