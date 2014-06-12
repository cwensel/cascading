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

package cascading.flow.hadoop.stream;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopCoGroupClosure;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.stream.Duct;
import cascading.flow.stream.GroupingSpliceGate;
import cascading.flow.stream.StreamGraph;
import cascading.pipe.CoGroup;
import cascading.tuple.Tuple;
import cascading.tuple.io.IndexTuple;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
public class HadoopCoGroupGate extends HadoopGroupGate
  {
  public HadoopCoGroupGate( FlowProcess flowProcess, CoGroup coGroup, GroupingSpliceGate.Role role )
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
  protected void wrapGroupingAndCollect( Duct previous, Tuple valuesTuple, Tuple groupKey ) throws java.io.IOException
    {
    Integer ordinal = ordinalMap.get( previous );

    collector.collect( new IndexTuple( ordinal, groupKey ), new IndexTuple( ordinal, valuesTuple ) );
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
