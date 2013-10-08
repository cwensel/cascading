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
  private boolean nullsAreOK;

  public TestIdentityBuffer( Fields groupFields, int numGroups, boolean nullsAreOK )
    {
    super( Fields.ARGS );
    this.groupFields = groupFields;
    this.numGroups = numGroups;
    this.nullsAreOK = nullsAreOK;
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

//    if( !group.getFields().equals( groupFields ) )
//      throw new RuntimeException( "group fields do not match: " + group.getFields() + " != " + groupFields );

    if( group.size() != groupFields.size() )
      throw new RuntimeException( "group tuple size not fields size" );

    if( group.size() == 0 )
      throw new RuntimeException( "group tuple size is zero" );

    boolean allAreNull = true;
    for( Object o : group.getTuple() )
      {
      if( o != null )
        allAreNull = false;
      }

    if( !nullsAreOK && allAreNull )
      throw new RuntimeException( "group tuple value is null" );

    Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();

    while( iterator.hasNext() )
      bufferCall.getOutputCollector().add( iterator.next() );
    }
  }