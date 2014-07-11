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

package cascading.flow.hadoop.stream.element;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.HadoopGroupByClosure;
import cascading.flow.hadoop.stream.HadoopGroupGate;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.graph.IORole;
import cascading.pipe.GroupBy;
import cascading.tuple.Tuple;
import cascading.tuple.io.TuplePair;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
public class HadoopGroupByGate extends HadoopGroupGate
  {
  public HadoopGroupByGate( FlowProcess flowProcess, GroupBy groupBy, IORole role )
    {
    super( flowProcess, groupBy, role );
    }

  @Override
  protected HadoopGroupByClosure createClosure()
    {
    // todo: collapse keyFields here if an array size > 1
    return new HadoopGroupByClosure( flowProcess, keyFields, valuesFields );
    }

  @Override
  protected void wrapGroupingAndCollect( Duct previous, Tuple valuesTuple, Tuple groupKey ) throws java.io.IOException
    {
    collector.collect( groupKey, valuesTuple );
    }

  @Override
  protected Tuple unwrapGrouping( Tuple key )
    {
    return sortFields == null ? key : ( (TuplePair) key ).getLhs();
    }

  protected OutputCollector createOutputCollector()
    {
    return ( (HadoopFlowProcess) flowProcess ).getOutputCollector();
    }
  }
