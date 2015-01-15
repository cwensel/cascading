/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
 */
public class InnerJoinTestBuffer extends BaseOperation implements Buffer
  {
  public InnerJoinTestBuffer( Fields declaredFields )
    {
    super( declaredFields );
    }

  @Override
  public void operate( FlowProcess flowProcess, BufferCall bufferCall )
    {
    JoinerClosure joinerClosure = bufferCall.getJoinerClosure();

    if( joinerClosure.size() != 2 )
      throw new IllegalArgumentException( "joiner size wrong" );

    Iterator<Tuple> lhs = joinerClosure.getIterator( 0 );

    while( lhs.hasNext() )
      {
      Tuple lhsTuple = lhs.next();

      Iterator<Tuple> rhs = joinerClosure.getIterator( 1 );

      while( rhs.hasNext() )
        {
        Tuple rhsTuple = rhs.next();

        Tuple result = new Tuple();

        result.addAll( lhsTuple );
        result.addAll( rhsTuple );

        bufferCall.getOutputCollector().add( result );
        }
      }
    }
  }
