/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream;

import java.util.Collection;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.pipe.cogroup.GroupClosure;
import cascading.tuple.Fields;

/**
 *
 */
public class CoGroupClosure extends GroupClosure
  {
  private Collection[] collections;
  private final int numSelfJoins;

  public CoGroupClosure( FlowProcess flowProcess, int numSelfJoins, Fields[] groupingFields, Fields[] valueFields )
    {
    super( flowProcess, groupingFields, valueFields );
    this.numSelfJoins = numSelfJoins;
    }

  @Override
  public int size()
    {
    return Math.max( groupingFields.length, numSelfJoins + 1 );
    }

  public void reset( Collection[] collections )
    {
    this.collections = collections;
    }

  @Override
  public Iterator getIterator( int pos )
    {
    if( numSelfJoins != 0 )
      return collections[ 0 ].iterator();
    else
      return collections[ pos ].iterator();
    }

  @Override
  public boolean isEmpty( int pos )
    {
    if( numSelfJoins != 0 )
      return collections[ 0 ].isEmpty();
    else
      return collections[ pos ].isEmpty();
    }
  }
