/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopMRUtil;
import cascading.flow.planner.BaseFlowNode;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementDirectedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.process.ProcessEdge;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import org.apache.hadoop.mapred.JobConf;

/** Class MapReduceFlowStep wraps a {@link JobConf} and allows it to be executed as a {@link cascading.flow.Flow}. */
public class MapReduceFlowStep extends HadoopFlowStep
  {
  public static final String MAP = "Map";
  public static final String SHUFFLE = "Shuffle";
  public static final String REDUCE = "Reduce";

  /** Field jobConf */
  private final JobConf jobConf;

  public MapReduceFlowStep( HadoopFlow flow, JobConf jobConf )
    {
    if( flow == null )
      throw new IllegalArgumentException( "flow may not be null" );

    setName( jobConf.getJobName() );
    setFlow( flow );

    this.jobConf = jobConf;

    configure(); // requires flow and jobConf
    }

  protected MapReduceFlowStep( HadoopFlow flow, String stepName, JobConf jobConf, Tap sink )
    {
    if( flow == null )
      throw new IllegalArgumentException( "flow may not be null" );

    setName( stepName );
    setFlow( flow );

    this.jobConf = jobConf;

    addSink( "default", sink );
    }

  protected JobConf getJobConf()
    {
    return jobConf;
    }

  @Override
  public ElementGraph getElementGraph()
    {
    if( elementGraph == null )
      elementGraph = createStepElementGraph( getFlowNodeGraph() );

    return elementGraph;
    }

  @Override
  public FlowNodeGraph getFlowNodeGraph()
    {
    if( flowNodeGraph == null )
      flowNodeGraph = createFlowNodeGraph( createNodeElementGraphs( jobConf ) );

    return flowNodeGraph;
    }

  @Override
  public JobConf createInitializedConfig( FlowProcess<JobConf> flowProcess, JobConf parentConfig )
    {
    return jobConf;
    }

  private ElementGraph createStepElementGraph( FlowNodeGraph flowNodeGraph )
    {
    return ElementGraphs.asElementDirectedGraph( flowNodeGraph.getElementGraphs() ).bindExtents();
    }

  private List<ElementGraph> createNodeElementGraphs( JobConf jobConf )
    {
    BaseMapReduceFlow baseFlow = (BaseMapReduceFlow) getFlow();
    boolean hasReducer = HadoopMRUtil.hasReducer( jobConf );

    List<ElementGraph> result = new ArrayList<>();
    ElementGraph mapElementGraph = createElementDirectedGraph();
    ElementGraph tailElementGraph = mapElementGraph;

    Pipe headOperation = createMapOperation();
    Pipe tailOperation = headOperation;

    mapElementGraph.addVertex( headOperation );

    result.add( mapElementGraph );

    ElementGraph reduceElementGraph = null;

    if( hasReducer )
      {
      Pipe shuffleOperation = createShuffleOperation();
      mapElementGraph.addVertex( shuffleOperation );

      mapElementGraph.addEdge( headOperation, shuffleOperation );

      reduceElementGraph = createElementDirectedGraph();

      reduceElementGraph.addVertex( shuffleOperation );
      Pipe reduceOperation = createReduceOperation();
      reduceElementGraph.addVertex( reduceOperation );

      reduceElementGraph.addEdge( shuffleOperation, reduceOperation );

      tailOperation = reduceOperation;
      tailElementGraph = reduceElementGraph;

      result.add( reduceElementGraph );
      }

    Map<String, Tap> sources = baseFlow.createSources( jobConf );

    for( Map.Entry<String, Tap> entry : sources.entrySet() )
      {
      mapElementGraph.addVertex( entry.getValue() );
      mapElementGraph.addEdge( entry.getValue(), headOperation, new Scope( entry.getKey() ) );
      }

    Map<String, Tap> sinks = baseFlow.createSinks( jobConf );

    for( Map.Entry<String, Tap> entry : sinks.entrySet() )
      {
      tailElementGraph.addVertex( entry.getValue() );
      tailElementGraph.addEdge( tailOperation, entry.getValue(), new Scope( entry.getKey() ) );
      }

    mapElementGraph.bindExtents();

    if( reduceElementGraph != null )
      reduceElementGraph.bindExtents();

    return result;
    }

  protected ElementDirectedGraph createElementDirectedGraph()
    {
    return new ElementDirectedGraph();
    }

  protected Pipe createMapOperation()
    {
    return new Pipe( MAP );
    }

  protected Pipe createShuffleOperation()
    {
    return new Pipe( SHUFFLE );
    }

  protected Pipe createReduceOperation()
    {
    return new Pipe( REDUCE );
    }

  protected FlowNodeGraph createFlowNodeGraph( List<ElementGraph> elementGraphs )
    {
    ElementGraph mapElementGraph = elementGraphs.get( 0 );
    ElementGraph reduceElementGraph = elementGraphs.size() == 2 ? elementGraphs.get( 1 ) : null;

    FlowNodeGraph flowNodeGraph = new FlowNodeGraph();
    int nodes = elementGraphs.size();

    FlowNode mapperNode = new BaseFlowNode( mapElementGraph, String.format( "(1/%s)", nodes ), 0 );
    flowNodeGraph.addVertex( mapperNode );

    if( nodes == 2 )
      {
      FlowNode reducerNode = new BaseFlowNode( reduceElementGraph, "(2/2)", 1 );
      flowNodeGraph.addVertex( reducerNode );
      flowNodeGraph.addEdge( mapperNode, reducerNode, new ProcessEdge( mapperNode, reducerNode ) );
      }

    return flowNodeGraph;
    }
  }
