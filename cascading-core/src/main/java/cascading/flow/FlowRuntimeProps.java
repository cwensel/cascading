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

package cascading.flow;

import java.util.Properties;

import cascading.property.Props;

/**
 * Class FlowRuntimeProps is a fluent helper class for setting {@link Flow} specific runtime properties through
 * a {@link FlowConnector}.
 * <p/>
 * These properties apply to the cluster or remote side of the Flow execution. For client (or local) side properties
 * see {@link cascading.flow.FlowProps}.
 * <p/>
 * Available properties are:
 * <p/>
 * <ul>
 * <li>gather partitions - number of slices (partitions) to gather keys within each {@link cascading.flow.FlowNode}.
 * In MapReduce this is the number of reducers. In Tez DAG this is the scatter gather parallelization.</li>
 * </ul>
 * <p/>
 * Note, if the num of gather partitions is not set, the Flow may fail during planning or setup, depending on the
 * platform.
 */
public class FlowRuntimeProps extends Props
  {
  public static final String GATHER_PARTITIONS = "cascading.flow.runtime.gather.partitions.num";

  int gatherPartitions = 0;

  public static FlowRuntimeProps flowRuntimeProps()
    {
    return new FlowRuntimeProps();
    }

  public FlowRuntimeProps()
    {
    }

  /**
   * Method getGatherPartitions returns the number of gather partitions
   *
   * @return number of gather partitions
   */
  public int getGatherPartitions()
    {
    return gatherPartitions;
    }

  /**
   * Method setGatherPartitions sets the default number of gather partitions each {@link cascading.flow.FlowNode}
   * should use.
   *
   * @param gatherPartitions number of gather partitions to use per node
   * @return this
   */
  public FlowRuntimeProps setGatherPartitions( int gatherPartitions )
    {
    if( gatherPartitions < 1 )
      throw new IllegalArgumentException( "gatherPartitions value must be greater than zero" );

    this.gatherPartitions = gatherPartitions;

    return this;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    properties.setProperty( GATHER_PARTITIONS, Integer.toString( gatherPartitions ) );
    }
  }
