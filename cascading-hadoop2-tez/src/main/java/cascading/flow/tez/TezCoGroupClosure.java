/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.tez;

import java.util.Collection;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopCoGroupClosure;
import cascading.flow.hadoop.util.LazySpillableTupleCollection;
import cascading.flow.hadoop.util.ResettableCollection;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
 */
public class TezCoGroupClosure extends HadoopCoGroupClosure
  {
  public TezCoGroupClosure( FlowProcess flowProcess, int numSelfJoins, Fields[] groupingFields, Fields[] valueFields )
    {
    super( flowProcess, numSelfJoins, groupingFields, valueFields );
    }

  protected void build()
    {
    clearGroups();

    for( int pos = 0; pos < values.length; pos++ )
      ( (ResettableCollection) collections[ pos ] ).reset( values[ pos ] );

    // todo: prevent an initial iteration to populate the lazy collection
    if( numSelfJoins != 0 ) // force fill of lazy collection
      {
      Iterator<Tuple> iterator = collections[ 0 ].iterator();

      while( iterator.hasNext() )
        iterator.next(); // do nothing, populates the lazy collection
      }
    }

  @Override
  protected Collection<Tuple> createTupleCollection( Fields joinField )
    {
    return new LazySpillableTupleCollection( super.createTupleCollection( joinField ) );
    }
  }
