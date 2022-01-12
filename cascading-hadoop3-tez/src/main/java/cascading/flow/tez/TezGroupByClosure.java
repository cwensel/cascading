/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopGroupByClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleChainIterator;

/**
 *
 */
public class TezGroupByClosure extends HadoopGroupByClosure
  {
  public TezGroupByClosure( FlowProcess flowProcess, Fields[] groupingFields, Fields[] valueFields )
    {
    super( flowProcess, groupingFields, valueFields );
    }

  @Override
  protected Iterator<Tuple> getValueIterator( int pos )
    {
    return new TupleChainIterator( values );
    }
  }
