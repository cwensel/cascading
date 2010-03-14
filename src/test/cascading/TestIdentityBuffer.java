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

package cascading;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class TestIdentityBuffer extends BaseOperation<Integer> implements Buffer<Integer>
  {
  private Fields groupFields;
  private Integer numGroups;

  public TestIdentityBuffer( Fields groupFields, int numGroups )
    {
    super( Fields.ARGS );
    this.groupFields = groupFields;
    this.numGroups = numGroups;
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Integer> operationCall )
    {
    operationCall.setContext( 0 );
    }


  @Override
  public void cleanup( FlowProcess flowProcess, OperationCall<Integer> operationCall )
    {
    if( !operationCall.getContext().equals( numGroups ) )
      throw new RuntimeException( "incorrect num groups, found: " + operationCall.getContext() + " expected: " + numGroups );
    }

  public void operate( FlowProcess flowProcess, BufferCall<Integer> bufferCall )
    {
    bufferCall.setContext( bufferCall.getContext() + 1 );
    TupleEntry group = bufferCall.getGroup();

    if( !group.getFields().equals( groupFields ) )
      throw new RuntimeException( "group fields do not match" );

    if( group.size() != groupFields.size() )
      throw new RuntimeException( "group tuple size not fields size" );

    if( group.size() == 0 )
      throw new RuntimeException( "group tuple size is zero" );

    for( Object o : group.getTuple() )
      {
      if( o == null )
        throw new RuntimeException( "group tuple value is null" );
      }

    Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();

    while( iterator.hasNext() )
      bufferCall.getOutputCollector().add( iterator.next() );
    }
  }