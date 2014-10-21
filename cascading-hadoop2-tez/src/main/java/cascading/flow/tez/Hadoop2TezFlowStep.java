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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowElements;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.hadoop.ConfigurationSetter;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.process.ProcessGraph;
import cascading.flow.stream.annotations.StreamMode;
import cascading.flow.tez.planner.Hadoop2TezFlowStepJob;
import cascading.management.state.ClientState;
import cascading.pipe.Boundary;
import cascading.pipe.CoGroup;
import cascading.pipe.Group;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.PartitionTap;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.util.GroupingSortingComparator;
import cascading.tuple.hadoop.util.ReverseGroupingSortingComparator;
import cascading.tuple.hadoop.util.ReverseTupleComparator;
import cascading.tuple.hadoop.util.TupleComparator;
import cascading.tuple.io.TuplePair;
import cascading.tuple.tez.util.GroupingSortingPartitioner;
import cascading.tuple.tez.util.TuplePartitioner;
import cascading.util.Util;
import cascading.util.Version;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValueInput;
import org.apache.tez.runtime.library.input.OrderedGroupedKVInput;
import org.apache.tez.runtime.library.input.OrderedGroupedMergedKVInput;
import org.apache.tez.runtime.library.input.UnorderedKVInput;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.output.UnorderedPartitionedKVOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.hadoop.util.HadoopUtil.addComparators;
import static cascading.flow.hadoop.util.HadoopUtil.serializeBase64;
import static cascading.flow.tez.util.TezUtil.addToClassPath;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.yarn.api.records.LocalResourceType.FILE;
import static org.apache.hadoop.yarn.api.records.LocalResourceType.PATTERN;

/**
 *
 */
public class Hadoop2TezFlowStep extends BaseFlowStep<TezConfiguration>
  {
  private static final Logger LOG = LoggerFactory.getLogger( Hadoop2TezFlowStep.class );

  private Map<String, LocalResource> localResources;
  private Map<Path, Path> syncPaths;
  private Map<String, String> environment;

  public Hadoop2TezFlowStep( ElementGraph elementGraph, FlowNodeGraph flowNodeGraph )
    {
    super( elementGraph, flowNodeGraph );
    }

  public Map<Path, Path> getSyncPaths()
    {
    return syncPaths;
    }

  @Override
  public TezConfiguration createInitializedConfig( FlowProcess<TezConfiguration> flowProcess, TezConfiguration parentConfig )
    {
    TezConfiguration stepConf = parentConfig == null ? new TezConfiguration() : new TezConfiguration( parentConfig );

    TupleSerialization.setSerializations( stepConf );

    String versionString = Version.getRelease();

    if( versionString != null )
      stepConf.set( "cascading.version", versionString );

    stepConf.set( CASCADING_FLOW_STEP_ID, getID() );
    stepConf.set( "cascading.flow.step.num", Integer.toString( getOrdinal() ) );

    HadoopUtil.setIsInflow( stepConf );

    String flowStagingPath = ( (Hadoop2TezFlow) getFlow() ).getFlowStagingPath();
    List<String> classPath = ( (Hadoop2TezFlow) getFlow() ).getClassPath();

    localResources = new HashMap<>();
    syncPaths = addToClassPath( stepConf, flowStagingPath, classPath, FILE, localResources, environment );

    environment = new HashMap<>();
    String appJarPath = stepConf.get( AppProps.APP_JAR_PATH );

    if( appJarPath != null )
      {
      List<String> classpath = singletonList( appJarPath );
      Map<Path, Path> pathMap = addToClassPath( stepConf, flowStagingPath, classpath, PATTERN, localResources, environment );

      syncPaths.putAll( pathMap );
      }

    return stepConf;
    }

  @Override
  protected FlowStepJob createFlowStepJob( ClientState clientState, FlowProcess<TezConfiguration> flowProcess, TezConfiguration initializedStepConfig )
    {
    DAG dag = createDAG( flowProcess, initializedStepConfig );

    return new Hadoop2TezFlowStepJob( clientState, this, initializedStepConfig, dag );
    }

  private DAG createDAG( FlowProcess<TezConfiguration> flowProcess, TezConfiguration initializedConfig )
    {
    FlowNodeGraph nodeGraph = getFlowNodeGraph();
    Map<FlowNode, Vertex> vertexMap = new HashMap<>();
    DAG dag = DAG.create( getStepDisplayName( initializedConfig.getInt( "cascading.display.id.truncate", Util.ID_LENGTH ) ) );

    dag.addTaskLocalFiles( localResources );

    Iterator<FlowNode> iterator = nodeGraph.getOrderedTopologicalIterator(); // ordering of nodes for consistent remote debugging

    while( iterator.hasNext() )
      {
      FlowNode flowNode = iterator.next();

      Vertex vertex = createVertex( flowProcess, initializedConfig, flowNode );
      dag.addVertex( vertex );

      vertexMap.put( flowNode, vertex );
      }

    LinkedList<ProcessGraph.ProcessEdge> processedEdges = new LinkedList<>();

    for( ProcessGraph.ProcessEdge processEdge : nodeGraph.edgeSet() )
      {
      if( processedEdges.contains( processEdge ) )
        continue;

      FlowNode edgeTargetFlowNode = nodeGraph.getEdgeTarget( processEdge );

      FlowElement flowElement = processEdge.getFlowElement();
      List<FlowNode> sourceNodes = nodeGraph.getElementSourceProcesses( flowElement );

      EdgeProperty edgeProperty = createEdgeProperty( initializedConfig, processEdge );

      Vertex targetVertex = vertexMap.get( edgeTargetFlowNode );

      if( sourceNodes.size() == 1 || flowElement instanceof CoGroup || flowElement instanceof Boundary ) // todo: create group vertices around incoming ordinal
        {
        FlowNode edgeSourceFlowNode = nodeGraph.getEdgeSource( processEdge );
        Vertex sourceVertex = vertexMap.get( edgeSourceFlowNode );

        LOG.debug( "adding edge between: {} and {}", sourceVertex, targetVertex );

        dag.addEdge( Edge.create( sourceVertex, targetVertex, edgeProperty ) );
        }
      else if( flowElement instanceof GroupBy || flowElement instanceof Merge ) // merge - source nodes > 1
        {
        List<String> sourceVerticesIDs = new ArrayList<>();
        List<Vertex> sourceVertices = new ArrayList<>();

        for( FlowNode edgeSourceFlowNode : sourceNodes )
          {
          sourceVerticesIDs.add( edgeSourceFlowNode.getID() );
          sourceVertices.add( vertexMap.get( edgeSourceFlowNode ) );
          processedEdges.add( nodeGraph.getEdge( edgeSourceFlowNode, edgeTargetFlowNode ) );
          }

        VertexGroup vertexGroup = dag.createVertexGroup( edgeTargetFlowNode.getID(), sourceVertices.toArray( new Vertex[ sourceVertices.size() ] ) );

        String inputClassName = flowElement instanceof Group ? OrderedGroupedMergedKVInput.class.getName() : ConcatenatedMergedKeyValueInput.class.getName();

        InputDescriptor inputDescriptor = InputDescriptor.create( inputClassName ).setUserPayload( edgeProperty.getEdgeDestination().getUserPayload() );

        LOG.info( "adding grouped edge between: {} and {}", Util.join( sourceVerticesIDs, "," ), targetVertex.getName() );
        dag.addEdge( GroupInputEdge.create( vertexGroup, targetVertex, edgeProperty, inputDescriptor ) );
        }
      else
        {
        throw new UnsupportedOperationException( "can't make edge for: " + flowElement );
        }
      }

    return dag;
    }

  private EdgeProperty createEdgeProperty( TezConfiguration config, ProcessGraph.ProcessEdge processEdge )
    {
    FlowElement flowElement = processEdge.getFlowElement();

    EdgeValues edgeValues = new EdgeValues( new TezConfiguration( config ), processEdge );

    edgeValues.keyClassName = Tuple.class.getName(); // TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_CLASS
    edgeValues.valueClassName = Tuple.class.getName(); // TEZ_RUNTIME_INTERMEDIATE_OUTPUT_VALUE_CLASS
    edgeValues.keyComparatorClassName = TupleComparator.class.getName();
    edgeValues.keyPartitionerClassName = TuplePartitioner.class.getName();
    edgeValues.outputClassName = null;
    edgeValues.inputClassName = null;
    edgeValues.movementType = null;
    edgeValues.sourceType = null;
    edgeValues.schedulingType = null;

    if( flowElement instanceof Group )
      applyGroup( edgeValues );
    else if( ( flowElement instanceof Boundary || flowElement instanceof Merge ) && processEdge.getSourceAnnotations().contains( StreamMode.Accumulated ) )
      applyBoundaryMergeAccumulated( edgeValues );
    else if( flowElement instanceof Boundary || flowElement instanceof Merge )
      applyBoundaryMerge( edgeValues );
    else
      throw new IllegalStateException( "unsupported flow element: " + flowElement.getClass().getCanonicalName() );

    return createEdgeProperty( edgeValues );
    }

  private EdgeValues applyBoundaryMerge( EdgeValues edgeValues )
    {
    // todo: support for one to one
    edgeValues.outputClassName = UnorderedPartitionedKVOutput.class.getName();
    edgeValues.inputClassName = UnorderedKVInput.class.getName();

    edgeValues.movementType = EdgeProperty.DataMovementType.SCATTER_GATHER;
    edgeValues.sourceType = EdgeProperty.DataSourceType.PERSISTED;
    edgeValues.schedulingType = EdgeProperty.SchedulingType.SEQUENTIAL;

    return edgeValues;
    }

  private EdgeValues applyBoundaryMergeAccumulated( EdgeValues edgeValues )
    {
    edgeValues.outputClassName = UnorderedKVOutput.class.getName();
    edgeValues.inputClassName = UnorderedKVInput.class.getName();

    edgeValues.movementType = EdgeProperty.DataMovementType.BROADCAST;
    edgeValues.sourceType = EdgeProperty.DataSourceType.PERSISTED;
    edgeValues.schedulingType = EdgeProperty.SchedulingType.SEQUENTIAL;

    return edgeValues;
    }

  private EdgeValues applyGroup( EdgeValues edgeValues )
    {
    Group group = (Group) edgeValues.flowElement;

    if( group.isSortReversed() )
      edgeValues.keyComparatorClassName = ReverseTupleComparator.class.getName();

    addComparators( edgeValues.config, "cascading.group.comparator", group.getKeySelectors(), this, group );

    if( !group.isGroupBy() )
      {
      edgeValues.outputClassName = OrderedPartitionedKVOutput.class.getName();
      edgeValues.inputClassName = OrderedGroupedKVInput.class.getName();

      edgeValues.movementType = EdgeProperty.DataMovementType.SCATTER_GATHER;
      edgeValues.sourceType = EdgeProperty.DataSourceType.PERSISTED;
      edgeValues.schedulingType = EdgeProperty.SchedulingType.SEQUENTIAL;
      }
    else
      {
      addComparators( edgeValues.config, "cascading.sort.comparator", group.getSortingSelectors(), this, group );

      edgeValues.outputClassName = OrderedPartitionedKVOutput.class.getName();
      edgeValues.inputClassName = OrderedGroupedKVInput.class.getName();

      edgeValues.movementType = EdgeProperty.DataMovementType.SCATTER_GATHER;
      edgeValues.sourceType = EdgeProperty.DataSourceType.PERSISTED;
      edgeValues.schedulingType = EdgeProperty.SchedulingType.SEQUENTIAL;
      }

    if( group.isSorted() )
      {
      edgeValues.keyClassName = TuplePair.class.getName();
      edgeValues.keyPartitionerClassName = GroupingSortingPartitioner.class.getName();

      if( group.isSortReversed() )
        edgeValues.keyComparatorClassName = ReverseGroupingSortingComparator.class.getName();
      else
        edgeValues.keyComparatorClassName = GroupingSortingComparator.class.getName();
      }

    return edgeValues;
    }

  private EdgeProperty createEdgeProperty( EdgeValues edgeValues )
    {
    TezConfiguration outputConfig = new TezConfiguration( edgeValues.getConfig() );
    outputConfig.set( "cascading.node.sink", FlowElements.id( edgeValues.getFlowElement() ) );

    UserPayload outputPayload = createIntermediatePayloadOutput( outputConfig, edgeValues );

    TezConfiguration inputConfig = new TezConfiguration( edgeValues.getConfig() );
    inputConfig.set( "cascading.node.source", FlowElements.id( edgeValues.getFlowElement() ) );
    inputConfig.set( "cascading.node.source.ordinals", Util.join( edgeValues.getOrdinals(), "," ) );

    UserPayload inputPayload = createIntermediatePayloadInput( inputConfig, edgeValues );

    return EdgeProperty.create(
      edgeValues.getMovementType(),
      edgeValues.getSourceType(),
      edgeValues.getSchedulingType(),
      OutputDescriptor.create( edgeValues.getOutputClassName() ).setUserPayload( outputPayload ),
      InputDescriptor.create( edgeValues.getInputClassName() ).setUserPayload( inputPayload )
    );
    }

  private UserPayload createIntermediatePayloadOutput( TezConfiguration config, EdgeValues edgeValues )
    {
    config.set( TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, edgeValues.keyClassName );
    config.set( TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, edgeValues.valueClassName );
    config.set( TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS, edgeValues.keyComparatorClassName );
    config.set( TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, edgeValues.keyPartitionerClassName );

    setWorkingDirectory( config );

    return getPayload( config );
    }

  private UserPayload createIntermediatePayloadInput( TezConfiguration config, EdgeValues edgeValues )
    {
    config.set( TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, edgeValues.keyClassName );
    config.set( TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, edgeValues.valueClassName );
    config.set( TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS, edgeValues.keyComparatorClassName );
    config.set( TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, edgeValues.keyPartitionerClassName );

    setWorkingDirectory( config );

    return getPayload( config );
    }

  private static void setWorkingDirectory( Configuration conf )
    {
    String name = conf.get( JobContext.WORKING_DIR );

    if( name != null )
      return;

    try
      {
      Path dir = FileSystem.get( conf ).getWorkingDirectory();
      conf.set( JobContext.WORKING_DIR, dir.toString() );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( exception );
      }
    }

  public Vertex createVertex( FlowProcess<TezConfiguration> flowProcess, TezConfiguration initializedConfig, FlowNode flowNode )
    {
    JobConf conf = new JobConf( initializedConfig );

    addInputOutputMapping( conf, flowNode );

    conf.setBoolean( "mapred.used.genericoptionsparser", true );

    Map<FlowElement, Configuration> sourceConfigs = initFromSources( flowNode, flowProcess, conf );
    Map<FlowElement, Configuration> sinkConfigs = initFromSinks( flowNode, flowProcess, conf );

    initFromTraps( flowNode, flowProcess, conf );

    initFromProcessConfigDef( flowNode, conf );

    // force step to local mode if any tap is local
    setLocalMode( initializedConfig, conf, null );

    conf.set( "cascading.flow.node.num", Integer.toString( flowNode.getOrdinal() ) );

    int parallelism = getParallelism( flowNode, conf );

    if( parallelism == 0 )
      throw new FlowException( getName(), "the default number of gather partitions must be set, see cascading.flow.FlowRuntimeProps" );

    Vertex vertex = newVertex( flowNode, conf, parallelism );

    for( FlowElement flowElement : sourceConfigs.keySet() )
      {
      if( !( flowElement instanceof Tap ) )
        continue;

      Configuration sourceConf = sourceConfigs.get( flowElement );
      MRInput.MRInputConfigBuilder configBuilder = MRInput.createConfigBuilder( sourceConf, null );

      // grouping splits loses file name info, breaking partition tap default impl
      if( flowElement instanceof PartitionTap ) // todo: generify
        configBuilder.groupSplits( false );

      DataSourceDescriptor dataSourceDescriptor = configBuilder.build();

      vertex.addDataSource( FlowElements.id( flowElement ), dataSourceDescriptor );
      }

    for( FlowElement flowElement : sinkConfigs.keySet() )
      {
      if( !( flowElement instanceof Tap ) )
        continue;

      Configuration sinkConf = sinkConfigs.get( flowElement );

      Class outputFormatClass;
      String outputPath;

      // we have to set sane defaults if not set by the tap
      // typically the case of MultiSinkTap
      String formatClassName = sinkConf.get( "mapred.output.format.class", sinkConf.get( MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR ) );

      if( formatClassName == null )
        {
        outputFormatClass = TextOutputFormat.class; // unused, use "new" api, its the default
        outputPath = Hfs.getTempPath( sinkConf ).toString(); // unused
        }
      else
        {
        outputFormatClass = Util.loadClass( formatClassName );
        outputPath = sinkConf.get( "mapred.output.dir" );
        }

      if( outputPath == null && sinkConf.get( "mapred.output.dir" ) == null )
        outputPath = Hfs.getTempPath( sinkConf ).toString(); // unused

      MROutput.MROutputConfigBuilder configBuilder = MROutput.createConfigBuilder( sinkConf, outputFormatClass, outputPath );

      DataSinkDescriptor dataSinkDescriptor = configBuilder.build();

      vertex.addDataSink( FlowElements.id( flowElement ), dataSinkDescriptor );
      }

    addRemoteDebug( flowNode, vertex );
    addRemoteProfiling( flowNode, vertex );

    return vertex;
    }

  protected int getParallelism( FlowNode flowNode, JobConf conf )
    {
    // only count streamed taps, accumulated taps are always annotated
    HashSet<Tap> sourceStreamedTaps = new HashSet<>( flowNode.getSourceTaps() );

    sourceStreamedTaps.removeAll( flowNode.getSourceElements( StreamMode.Accumulated ) );

    if( sourceStreamedTaps.size() != 0 )
      return -1;

    int parallelism = Integer.MAX_VALUE;

    for( Tap tap : flowNode.getSinkTaps() )
      {
      int numSinkParts = tap.getScheme().getNumSinkParts();

      if( numSinkParts == 0 )
        continue;

      if( parallelism != Integer.MAX_VALUE )
        LOG.info( "multiple sink taps in flow node declaring numSinkParts, choosing lowest value. see cascading.flow.FlowRuntimeProps for broader control." );

      parallelism = Math.min( parallelism, numSinkParts );
      }

    if( parallelism != Integer.MAX_VALUE )
      return parallelism;

    return conf.getInt( FlowRuntimeProps.GATHER_PARTITIONS, 0 );
    }

  private void addInputOutputMapping( JobConf conf, FlowNode flowNode )
    {
    FlowNodeGraph flowNodeGraph = getFlowNodeGraph();
    Set<ProcessGraph.ProcessEdge> incomingEdges = flowNodeGraph.incomingEdgesOf( flowNode );

    for( ProcessGraph.ProcessEdge processEdge : incomingEdges )
      conf.set( "cascading.node.source." + processEdge.getID(), flowNodeGraph.getEdgeSource( processEdge ).getID() );

    Set<ProcessGraph.ProcessEdge> outgoingEdges = flowNodeGraph.outgoingEdgesOf( flowNode );

    for( ProcessGraph.ProcessEdge processEdge : outgoingEdges )
      conf.set( "cascading.node.sink." + processEdge.getID(), flowNodeGraph.getEdgeTarget( processEdge ).getID() );
    }

  protected Map<FlowElement, Configuration> initFromSources( FlowNode flowNode, FlowProcess<TezConfiguration> flowProcess, Configuration conf )
    {
    Set<? extends FlowElement> accumulatedSources = flowNode.getSourceElements( StreamMode.Accumulated );

    for( FlowElement element : accumulatedSources )
      {
      if( element instanceof Tap )
        {
        JobConf current = new JobConf( conf );
        Tap tap = (Tap) element;

        if( tap.getIdentifier() == null )
          throw new IllegalStateException( "tap may not have null identifier: " + tap.toString() );

        tap.sourceConfInit( flowProcess, current );

        Map<String, String> map = flowProcess.diffConfigIntoMap( new TezConfiguration( conf ), new TezConfiguration( current ) );
        conf.set( "cascading.node.accumulated.source.conf." + Tap.id( tap ), pack( map, conf ) );

        setLocalMode( conf, current, tap );
        }
      }

    Set<FlowElement> sources = new HashSet<>( flowNode.getSourceElements() );

    sources.removeAll( accumulatedSources );

    if( sources.isEmpty() )
      throw new IllegalStateException( "all sources marked as accumulated" );

    Map<FlowElement, Configuration> configs = new HashMap<>();

    for( FlowElement element : sources )
      {
      JobConf current = new JobConf( conf );

      if( element instanceof Tap )
        {
        Tap tap = (Tap) element;

        if( tap.getIdentifier() == null )
          throw new IllegalStateException( "tap may not have null identifier: " + tap.toString() );

        tap.sourceConfInit( flowProcess, current );

        setLocalMode( conf, current, tap );
        }

      String id = FlowElements.id( element );

      current.set( "cascading.node.source", id );

      configs.put( element, current );
      }

    return configs;
    }

  protected Map<FlowElement, Configuration> initFromSinks( FlowNode flowNode, FlowProcess<? extends Configuration> flowProcess, Configuration conf )
    {
    Set<FlowElement> sinks = flowNode.getSinkElements();
    Map<FlowElement, Configuration> configs = new HashMap<>();

    for( FlowElement element : sinks )
      {
      JobConf current = new JobConf( conf );

      if( element instanceof Tap )
        {
        Tap tap = (Tap) element;

        if( tap.getIdentifier() == null )
          throw new IllegalStateException( "tap may not have null identifier: " + element.toString() );

        tap.sinkConfInit( flowProcess, current );

        setLocalMode( conf, current, tap );
        }

      String id = FlowElements.id( element );

      current.set( "cascading.node.sink", id );

      configs.put( element, current );
      }

    return configs;
    }

  private void initFromProcessConfigDef( FlowNode flowNode, Configuration conf )
    {
    initConfFromProcessConfigDef( flowNode.getElementGraph(), new ConfigurationSetter( conf ) );
    }

  protected void initFromTraps( FlowNode flowNode, FlowProcess<? extends Configuration> flowProcess, Configuration conf )
    {
    Map<String, Tap> traps = flowNode.getTrapMap();

    if( !traps.isEmpty() )
      {
      JobConf trapConf = new JobConf( conf );

      for( Tap tap : traps.values() )
        {
        tap.sinkConfInit( flowProcess, trapConf );
        setLocalMode( conf, trapConf, tap );
        }
      }
    }

  private Vertex newVertex( FlowNode flowNode, Configuration conf, int parallelism )
    {
    conf.set( FlowNode.CASCADING_FLOW_NODE, pack( flowNode, conf ) ); // todo: pack into payload directly

    ProcessorDescriptor descriptor = ProcessorDescriptor.create( FlowProcessor.class.getName() );

    descriptor.setUserPayload( getPayload( conf ) );

    Vertex vertex = Vertex.create( flowNode.getID(), descriptor, parallelism );

    if( environment != null )
      vertex.setTaskEnvironment( environment );

    return vertex;
    }

  private UserPayload getPayload( Configuration conf )
    {
    try
      {
      return TezUtils.createUserPayloadFromConf( conf );
      }
    catch( IOException exception )
      {
      throw new CascadingException( exception );
      }
    }

  private String pack( Object object, Configuration conf )
    {
    try
      {
      return serializeBase64( object, conf, true );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to pack object: " + object.getClass().getCanonicalName(), exception );
      }
    }

  @Override
  public void clean( TezConfiguration entries )
    {

    }

  private void setLocalMode( Configuration parent, JobConf current, Tap tap )
    {
    // force step to local mode
    if( !HadoopUtil.isLocal( current ) )
      return;

    if( tap != null )
      logInfo( "tap forcing step to tez local mode: " + tap.getIdentifier() );

    HadoopUtil.setLocal( parent );
    }

  private void addRemoteDebug( FlowNode flowNode, Vertex vertex )
    {
    String value = System.getProperty( "test.debug.node", null );

    if( Util.isEmpty( value ) )
      return;

    if( !flowNode.getSourceElementNames().contains( value ) && asInt( value ) != flowNode.getOrdinal() )
      return;

    LOG.warn( "remote debugging enabled with property: {}, on node: {}, with node id: {}", "test.debug.node", value, flowNode.getID() );

    String opts = vertex.getTaskLaunchCmdOpts();

    if( opts == null )
      opts = "";

    opts += " -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005,suspend=y";

    vertex.setTaskLaunchCmdOpts( opts );
    }

  private void addRemoteProfiling( FlowNode flowNode, Vertex vertex )
    {
    String value = System.getProperty( "test.profile.node", null );

    if( Util.isEmpty( value ) )
      return;

    if( !flowNode.getSourceElementNames().contains( value ) && asInt( value ) != flowNode.getOrdinal() )
      return;

    LOG.warn( "remote profiling enabled with property: {}, on node: {}, with node id: {}", "test.profile.node", value, flowNode.getID() );

    String opts = vertex.getTaskLaunchCmdOpts();

    if( opts == null )
      opts = "";

    String path = System.getProperty( "test.profile.path", "/tmp/jfr/" );

    if( !path.endsWith( "/" ) )
      path += "/";

    LOG.warn( "remote profiling property: {}, logging to path: {}", "test.profile.path", path );

    opts += String.format( " -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true,dumponexitpath=%1$s%2$s,disk=true,repository=%1$s%2$s", path, flowNode.getID() );

    vertex.setTaskLaunchCmdOpts( opts );
    }

  private int asInt( String value )
    {
    try
      {
      return Integer.parseInt( value );
      }
    catch( NumberFormatException exception )
      {
      return -1;
      }
    }

  private static class EdgeValues
    {
    FlowElement flowElement;
    TezConfiguration config;
    Set ordinals;
    String keyClassName;
    String valueClassName;
    String keyComparatorClassName;
    String keyPartitionerClassName;
    String outputClassName;
    String inputClassName;
    EdgeProperty.DataMovementType movementType;
    EdgeProperty.DataSourceType sourceType;
    EdgeProperty.SchedulingType schedulingType;

    private EdgeValues( TezConfiguration config, ProcessGraph.ProcessEdge processEdge )
      {
      this.config = config;
      this.flowElement = processEdge.getFlowElement();
      this.ordinals = processEdge.getIncomingOrdinals();
      }

    public FlowElement getFlowElement()
      {
      return flowElement;
      }

    public TezConfiguration getConfig()
      {
      return config;
      }

    public Set getOrdinals()
      {
      return ordinals;
      }

    public String getKeyClassName()
      {
      return keyClassName;
      }

    public String getValueClassName()
      {
      return valueClassName;
      }

    public String getKeyComparatorClassName()
      {
      return keyComparatorClassName;
      }

    public String getKeyPartitionerClassName()
      {
      return keyPartitionerClassName;
      }

    public String getOutputClassName()
      {
      return outputClassName;
      }

    public String getInputClassName()
      {
      return inputClassName;
      }

    public EdgeProperty.DataMovementType getMovementType()
      {
      return movementType;
      }

    public EdgeProperty.DataSourceType getSourceType()
      {
      return sourceType;
      }

    public EdgeProperty.SchedulingType getSchedulingType()
      {
      return schedulingType;
      }
    }
  }
