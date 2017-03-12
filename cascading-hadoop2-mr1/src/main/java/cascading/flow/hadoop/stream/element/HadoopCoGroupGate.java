/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.hadoop.stream.element;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopCoGroupClosure;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.stream.HadoopGroupGate;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.CoGroup;
import cascading.tuple.Tuple;
import cascading.tuple.io.IndexTuple;
import cascading.tuple.io.KeyIndexTuple;
import cascading.tuple.io.ValueIndexTuple;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
public class HadoopCoGroupGate extends HadoopGroupGate
  {
  IndexTuple keyTuple = new KeyIndexTuple();
  IndexTuple valueTuple = new ValueIndexTuple();

  public HadoopCoGroupGate( FlowProcess flowProcess, CoGroup coGroup, IORole role )
    {
    super( flowProcess, coGroup, role );
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    super.bind( streamGraph );
    }

  @Override
  protected HadoopCoGroupClosure createClosure()
    {
    return new HadoopCoGroupClosure( flowProcess, splice.getNumSelfJoins(), keyFields, valuesFields );
    }

  @Override
  protected void wrapGroupingAndCollect( Duct previous, int ordinal, Tuple valuesTuple, Tuple groupKey ) throws java.io.IOException
    {
    keyTuple.setIndex( ordinal );
    keyTuple.setTuple( groupKey );

    valueTuple.setIndex( ordinal );
    valueTuple.setTuple( valuesTuple );

    collector.collect( keyTuple, valueTuple );
    }

  @Override
  protected Tuple unwrapGrouping( Tuple key )
    {
    return ( (IndexTuple) key ).getTuple();
    }

  protected OutputCollector createOutputCollector()
    {
    return ( (HadoopFlowProcess) flowProcess ).getOutputCollector();
    }
  }
