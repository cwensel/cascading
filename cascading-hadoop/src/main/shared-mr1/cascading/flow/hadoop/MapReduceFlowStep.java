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

package cascading.flow.hadoop;

import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopMRUtil;
import cascading.flow.planner.BaseFlowNode;
import cascading.flow.planner.graph.BaseElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.process.ProcessEdge;
import cascading.tap.Tap;
import org.apache.hadoop.mapred.JobConf;

/** Class MapReduceFlowStep wraps a {@link JobConf} and allows it to be executed as a {@link cascading.flow.Flow}. */
public class MapReduceFlowStep extends HadoopFlowStep
  {
  /** Field jobConf */
  private final JobConf jobConf;

  protected MapReduceFlowStep( String flowName, String stepName, JobConf jobConf, Tap sink )
    {
    super( BaseElementGraph.NULL, createFlowNodeGraph( jobConf ) );
    setName( stepName );
    setFlowName( flowName );
    this.jobConf = jobConf;
    addSink( "default", sink );
    }

  @Override
  public JobConf createInitializedConfig( FlowProcess<JobConf> flowProcess, JobConf parentConfig )
    {
    // allow to delete
    getSink().sinkConfInit( flowProcess, new JobConf() );

    return jobConf;
    }

  private static FlowNodeGraph createFlowNodeGraph( JobConf jobConf )
    {
    FlowNodeGraph flowNodeGraph = new FlowNodeGraph();
    int nodes = 1;
    boolean hasReducer = HadoopMRUtil.hasReducer( jobConf );

    if( hasReducer )
      nodes = 2;

    FlowNode mapperNode = new BaseFlowNode( String.format( "(1/%s)", nodes ), 0 );
    flowNodeGraph.addVertex( mapperNode );

    if( hasReducer )
      {
      FlowNode reducerNode = new BaseFlowNode( "(2/2)", 1 );
      flowNodeGraph.addVertex( reducerNode );
      flowNodeGraph.addEdge( mapperNode, reducerNode, new ProcessEdge( mapperNode, reducerNode ) );
      }

    return flowNodeGraph;
    }
  }
