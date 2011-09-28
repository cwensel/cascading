/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe.cogroup;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/** Class GroupClosure is used internally to represent groups of tuples during grouping. */
public class GroupByClosure extends GroupClosure
  {
  Iterator values;

  public GroupByClosure( FlowProcess flowProcess, Fields[] groupingFields, Fields[] valueFields )
    {
    super( flowProcess, groupingFields, valueFields );
    }

  public int size()
    {
    return 1;
    }

  @Override
  public Iterator getIterator( int pos )
    {
    if( pos != 0 )
      throw new IllegalArgumentException( "invalid group position: " + pos );

    return makeIterator( 0, values );
    }

  @Override
  public boolean isEmpty( int pos )
    {
    return values != null;
    }

  protected Iterator<Tuple> makeIterator( final int pos, final Iterator values )
    {
    return new Iterator<Tuple>()
    {
    final int cleanPos = valueFields.length == 1 ? 0 : pos; // support repeated pipes

    public boolean hasNext()
      {
      return values.hasNext();
      }

    public Tuple next()
      {
      Tuple tuple = (Tuple) values.next();

      // todo: cache pos
      tuple.set( valueFields[ cleanPos ], groupingFields[ cleanPos ], grouping );

      return tuple;
      }

    public void remove()
      {
      throw new UnsupportedOperationException( "remove not supported" );
      }
    };
    }

  public void reset( Tuple grouping, Iterator values )
    {
    this.grouping = grouping;
    this.values = values;
    }
  }
