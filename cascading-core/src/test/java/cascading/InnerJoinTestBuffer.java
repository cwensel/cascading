/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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
