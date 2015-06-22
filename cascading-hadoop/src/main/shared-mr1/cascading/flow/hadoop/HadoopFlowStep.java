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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.hadoop.planner.HadoopFlowStepJob;
import cascading.flow.hadoop.util.HadoopMRUtil;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.process.ProcessEdge;
import cascading.management.state.ClientState;
import cascading.pipe.CoGroup;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.MultiInputFormat;
import cascading.tap.hadoop.util.Hadoop18TapUtil;
import cascading.tap.hadoop.util.TempHfs;
import cascading.tuple.Fields;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.util.CoGroupingComparator;
import cascading.tuple.hadoop.util.CoGroupingPartitioner;
import cascading.tuple.hadoop.util.GroupingComparator;
import cascading.tuple.hadoop.util.GroupingPartitioner;
import cascading.tuple.hadoop.util.GroupingSortingComparator;
import cascading.tuple.hadoop.util.GroupingSortingPartitioner;
import cascading.tuple.hadoop.util.IndexTupleCoGroupingComparator;
import cascading.tuple.hadoop.util.ReverseGroupingSortingComparator;
import cascading.tuple.hadoop.util.ReverseTupleComparator;
import cascading.tuple.hadoop.util.TupleComparator;
import cascading.tuple.io.KeyIndexTuple;
import cascading.tuple.io.KeyTuple;
import cascading.tuple.io.TuplePair;
import cascading.tuple.io.ValueIndexTuple;
import cascading.tuple.io.ValueTuple;
import cascading.util.ProcessLogger;
import cascading.util.Util;
import cascading.util.Version;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

import static cascading.flow.hadoop.util.HadoopUtil.*;

/**
 *
 */
public class HadoopFlowStep extends BaseFlowStep<JobConf>
  {
  protected HadoopFlowStep( String name, int ordinal )
    {
    super( name, ordinal );
    }

  public HadoopFlowStep( ElementGraph elementGraph, FlowNodeGraph flowNodeGraph )
    {
    super( elementGraph, flowNodeGraph );
    }

  @Override
  public Map<Object, Object> getConfigAsProperties()
    {
    return HadoopUtil.createProperties( getConfig() );
    }

  public JobConf createInitializedConfig( FlowProcess<JobConf> flowProcess, JobConf parentConfig )
    {
    JobConf conf = parentConfig == null ? new JobConf() : HadoopUtil.copyJobConf( parentConfig );

    // disable warning
    conf.setBoolean( "mapred.used.genericoptionsparser", true );

    conf.setJobName( getStepDisplayName( conf.getInt( "cascading.display.id.truncate", Util.ID_LENGTH ) ) );

    conf.setOutputKeyClass( KeyTuple.class );
    conf.setOutputValueClass( ValueTuple.class );

    conf.setMapRunnerClass( FlowMapper.class );
    conf.setReducerClass( FlowReducer.class );

    // set for use by the shuffling phase
    TupleSerialization.setSerializations( conf );

    initFromSources( flowProcess, conf );

    initFromSink( flowProcess, conf );

    initFromTraps( flowProcess, conf );

    initFromStepConfigDef( conf );

    int numSinkParts = getSink().getScheme().getNumSinkParts();

    if( numSinkParts != 0 )
      {
      // if no reducer, set num map tasks to control parts
      if( getGroup() != null )
        conf.setNumReduceTasks( numSinkParts );
      else
        conf.setNumMapTasks( numSinkParts );
      }
    else if( getGroup() != null )
      {
      int gatherPartitions = conf.getNumReduceTasks();

      if( gatherPartitions == 0 )
        gatherPartitions = conf.getInt( FlowRuntimeProps.GATHER_PARTITIONS, 0 );

      if( gatherPartitions == 0 )
        throw new FlowException( getName(), "a default number of gather partitions must be set, see FlowRuntimeProps" );

      conf.setNumReduceTasks( gatherPartitions );
      }

    conf.setOutputKeyComparatorClass( TupleComparator.class );

    ProcessEdge processEdge = Util.getFirst( getFlowNodeGraph().edgeSet() );

    if( getGroup() == null )
      {
      conf.setNumReduceTasks( 0 ); // disable reducers
      }
    else
      {
      // must set map output defaults when performing a reduce
      conf.setMapOutputKeyClass( KeyTuple.class );
      conf.setMapOutputValueClass( ValueTuple.class );
      conf.setPartitionerClass( GroupingPartitioner.class );

      // handles the case the groupby sort should be reversed
      if( getGroup().isSortReversed() )
        conf.setOutputKeyComparatorClass( ReverseTupleComparator.class );

      Integer ordinal = (Integer) Util.getFirst( processEdge.getIncomingOrdinals() );

      addComparators( conf, "cascading.group.comparator", getGroup().getKeySelectors(), (Fields) processEdge.getResolvedKeyFields().get( ordinal ) );

      if( getGroup().isGroupBy() )
        addComparators( conf, "cascading.sort.comparator", getGroup().getSortingSelectors(), (Fields) processEdge.getResolvedSortFields().get( ordinal ) );

      if( !getGroup().isGroupBy() )
        {
        conf.setPartitionerClass( CoGroupingPartitioner.class );
        conf.setMapOutputKeyClass( KeyIndexTuple.class ); // allows groups to be sorted by index
        conf.setMapOutputValueClass( ValueIndexTuple.class );
        conf.setOutputKeyComparatorClass( IndexTupleCoGroupingComparator.class ); // sorts by group, then by index
        conf.setOutputValueGroupingComparator( CoGroupingComparator.class );
        }

      if( getGroup().isSorted() )
        {
        conf.setPartitionerClass( GroupingSortingPartitioner.class );
        conf.setMapOutputKeyClass( TuplePair.class );

        if( getGroup().isSortReversed() )
          conf.setOutputKeyComparatorClass( ReverseGroupingSortingComparator.class );
        else
          conf.setOutputKeyComparatorClass( GroupingSortingComparator.class );

        // no need to supply a reverse comparator, only equality is checked
        conf.setOutputValueGroupingComparator( GroupingComparator.class );
        }
      }

    // if we write type information into the stream, we can perform comparisons in indexed tuples
    // thus, if the edge is a CoGroup and they keys are not common types, force writing of type information
    if( processEdge != null && ifCoGroupAndKeysHaveCommonTypes( this, processEdge.getFlowElement(), processEdge.getResolvedKeyFields() ) )
      {
      conf.set( "cascading.node.ordinals", Util.join( processEdge.getIncomingOrdinals(), "," ) );
      addFields( conf, "cascading.node.key.fields", processEdge.getResolvedKeyFields() );
      addFields( conf, "cascading.node.sort.fields", processEdge.getResolvedSortFields() );
      addFields( conf, "cascading.node.value.fields", processEdge.getResolvedValueFields() );
      }

    // perform last so init above will pass to tasks
    String versionString = Version.getRelease();

    if( versionString != null )
      conf.set( "cascading.version", versionString );

    conf.set( CASCADING_FLOW_STEP_ID, getID() );
    conf.set( "cascading.flow.step.num", Integer.toString( getOrdinal() ) );

    HadoopUtil.setIsInflow( conf );

    Iterator<FlowNode> iterator = getFlowNodeGraph().getTopologicalIterator();

    String mapState = pack( iterator.next(), conf );
    String reduceState = pack( iterator.hasNext() ? iterator.next() : null, conf );

    // hadoop 20.2 doesn't like dist cache when using local mode
    int maxSize = Short.MAX_VALUE;

    int length = mapState.length() + reduceState.length();

    if( isHadoopLocalMode( conf ) || length < maxSize ) // seems safe
      {
      conf.set( "cascading.flow.step.node.map", mapState );

      if( !Util.isEmpty( reduceState ) )
        conf.set( "cascading.flow.step.node.reduce", reduceState );
      }
    else
      {
      conf.set( "cascading.flow.step.node.map.path", HadoopMRUtil.writeStateToDistCache( conf, getID(), "map", mapState ) );

      if( !Util.isEmpty( reduceState ) )
        conf.set( "cascading.flow.step.node.reduce.path", HadoopMRUtil.writeStateToDistCache( conf, getID(), "reduce", reduceState ) );
      }

    return conf;
    }

  private static boolean ifCoGroupAndKeysHaveCommonTypes( ProcessLogger processLogger, FlowElement flowElement, Map<Integer, Fields> resolvedKeyFields )
    {
    if( !( flowElement instanceof CoGroup ) )
      return true;

    if( resolvedKeyFields == null || resolvedKeyFields.size() < 2 )
      return true;

    Iterator<Map.Entry<Integer, Fields>> iterator = resolvedKeyFields.entrySet().iterator();

    Fields fields = iterator.next().getValue();

    while( iterator.hasNext() )
      {
      Fields next = iterator.next().getValue();

      if( !Arrays.equals( fields.getTypesClasses(), next.getTypesClasses() ) )
        {
        processLogger.logWarn( "unable to perform: {}, on mismatched join types and optimize serialization with type exclusion, fields: {} & {}", flowElement, fields, next );
        return false;
        }
      }

    return true;
    }

  public boolean isHadoopLocalMode( JobConf conf )
    {
    return HadoopUtil.isLocal( conf );
    }

  protected FlowStepJob<JobConf> createFlowStepJob( ClientState clientState, FlowProcess<JobConf> flowProcess, JobConf initializedStepConfig )
    {
    try
      {
      return new HadoopFlowStepJob( clientState, this, initializedStepConfig );
      }
    catch( NoClassDefFoundError error )
      {
      PlatformInfo platformInfo = HadoopUtil.getPlatformInfo();
      String message = "unable to load platform specific class, please verify Hadoop cluster version: '%s', matches the Hadoop platform build dependency and associated FlowConnector, cascading-hadoop or cascading-hadoop2-mr1";

      logError( String.format( message, platformInfo.toString() ), error );

      throw error;
      }
    }

  /**
   * Method clean removes any temporary files used by this FlowStep instance. It will log any IOExceptions thrown.
   *
   * @param config of type JobConf
   */
  public void clean( JobConf config )
    {
    String stepStatePath = config.get( "cascading.flow.step.path" );

    if( stepStatePath != null )
      {
      try
        {
        HadoopUtil.removeStateFromDistCache( config, stepStatePath );
        }
      catch( IOException exception )
        {
        logWarn( "unable to remove step state file: " + stepStatePath, exception );
        }
      }

    if( tempSink != null )
      {
      try
        {
        tempSink.deleteResource( config );
        }
      catch( Exception exception )
        {
        // sink all exceptions, don't fail app
        logWarn( "unable to remove temporary file: " + tempSink, exception );
        }
      }

    if( getSink().isTemporary() && ( getFlow().getFlowStats().isSuccessful() || getFlow().getRunID() == null ) )
      {
      try
        {
        getSink().deleteResource( config );
        }
      catch( Exception exception )
        {
        // sink all exceptions, don't fail app
        logWarn( "unable to remove temporary file: " + getSink(), exception );
        }
      }
    else
      {
      cleanTapMetaData( config, getSink() );
      }

    for( Tap tap : getTraps() )
      cleanTapMetaData( config, tap );
    }

  private void cleanTapMetaData( JobConf jobConf, Tap tap )
    {
    try
      {
      Hadoop18TapUtil.cleanupTapMetaData( jobConf, tap );
      }
    catch( IOException exception )
      {
      // ignore exception
      }
    }

  private void initFromTraps( FlowProcess<JobConf> flowProcess, JobConf conf, Map<String, Tap> traps )
    {
    if( !traps.isEmpty() )
      {
      JobConf trapConf = HadoopUtil.copyJobConf( conf );

      for( Tap tap : traps.values() )
        tap.sinkConfInit( flowProcess, trapConf );
      }
    }

  protected void initFromSources( FlowProcess<JobConf> flowProcess, JobConf conf )
    {
    // handles case where same tap is used on multiple branches
    // we do not want to init the same tap multiple times
    Set<Tap> uniqueSources = getUniqueStreamedSources();

    JobConf[] streamedJobs = new JobConf[ uniqueSources.size() ];
    int i = 0;

    for( Tap tap : uniqueSources )
      {
      if( tap.getIdentifier() == null )
        throw new IllegalStateException( "tap may not have null identifier: " + tap.toString() );

      streamedJobs[ i ] = flowProcess.copyConfig( conf );

      streamedJobs[ i ].set( "cascading.step.source", Tap.id( tap ) );

      tap.sourceConfInit( flowProcess, streamedJobs[ i ] );

      i++;
      }

    Set<Tap> accumulatedSources = getAllAccumulatedSources();

    for( Tap tap : accumulatedSources )
      {
      JobConf accumulatedJob = flowProcess.copyConfig( conf );

      tap.sourceConfInit( flowProcess, accumulatedJob );

      Map<String, String> map = flowProcess.diffConfigIntoMap( conf, accumulatedJob );
      conf.set( "cascading.node.accumulated.source.conf." + Tap.id( tap ), pack( map, conf ) );

      try
        {
        if( DistributedCache.getCacheFiles( accumulatedJob ) != null )
          DistributedCache.setCacheFiles( DistributedCache.getCacheFiles( accumulatedJob ), conf );
        }
      catch( IOException exception )
        {
        throw new CascadingException( exception );
        }
      }

    MultiInputFormat.addInputFormat( conf, streamedJobs ); //must come last
    }

  private void initFromStepConfigDef( final JobConf conf )
    {
    initConfFromStepConfigDef( new ConfigurationSetter( conf ) );
    }

  /**
   * sources are specific to step, remove all known accumulated sources, if any
   *
   * @return
   */
  private Set<Tap> getUniqueStreamedSources()
    {
    Set<Tap> allAccumulatedSources = getAllAccumulatedSources();

    HashSet<Tap> set = new HashSet<>( sources.keySet() );

    set.removeAll( allAccumulatedSources );

    return set;
    }

  protected void initFromSink( FlowProcess<JobConf> flowProcess, JobConf conf )
    {
    // init sink first so tempSink can take precedence
    if( getSink() != null )
      getSink().sinkConfInit( flowProcess, conf );

    Class<? extends OutputFormat> outputFormat = conf.getClass( "mapred.output.format.class", null, OutputFormat.class );
    boolean isFileOutputFormat = false;

    if( outputFormat != null )
      isFileOutputFormat = FileOutputFormat.class.isAssignableFrom( outputFormat );

    Path outputPath = FileOutputFormat.getOutputPath( conf );

    // if no output path is set, we need to substitute an alternative if the OutputFormat is file based
    // PartitionTap won't set the output, but will set an OutputFormat
    // MultiSinkTap won't set the output or set the OutputFormat
    // Non file based OutputFormats don't have an output path, but do have an OutputFormat set (JDBCTap..)
    if( outputPath == null && ( isFileOutputFormat || outputFormat == null ) )
      tempSink = new TempHfs( conf, "tmp:/" + new Path( getSink().getIdentifier() ).toUri().getPath(), true );

    // tempSink exists because sink is writeDirect
    if( tempSink != null )
      tempSink.sinkConfInit( flowProcess, conf );
    }

  protected void initFromTraps( FlowProcess<JobConf> flowProcess, JobConf conf )
    {
    initFromTraps( flowProcess, conf, getTrapMap() );
    }
  }
