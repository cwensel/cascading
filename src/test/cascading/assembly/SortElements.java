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

package cascading.assembly;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/** Class SortElements ... */
public class SortElements extends BaseOperation implements Function
  {
  private final Fields[] fields;

  public SortElements( Fields... fields )
    {
    super( Fields.ARGS );
    this.fields = fields;
    }

  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    Set<Tuple> set = new TreeSet<Tuple>();

    TupleEntry input = functionCall.getArguments();

    for( Fields field : fields )
      set.add( input.selectTuple( field ) );

    int i = 0;
    Tuple inputCopy = new Tuple( input.getTuple() );

    for( Tuple tuple : set )
      inputCopy.put( input.getFields(), fields[ i++ ], tuple );

    functionCall.getOutputCollector().add( inputCopy );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof SortElements ) )
      return false;
    if( !super.equals( object ) )
      return false;

    SortElements that = (SortElements) object;

    if( !Arrays.equals( fields, that.fields ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( fields != null ? Arrays.hashCode( fields ) : 0 );
    return result;
    }
  }
