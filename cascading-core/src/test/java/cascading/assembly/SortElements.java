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
