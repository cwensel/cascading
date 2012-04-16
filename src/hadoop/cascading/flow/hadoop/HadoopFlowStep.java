/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.flow.hadoop.planner.HadoopFlowStepJob;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.pipe.ConfigDef;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hadoop18TapUtil;
import cascading.tap.hadoop.MultiInputFormat;
import cascading.tap.hadoop.TempHfs;
import cascading.tuple.Fields;
import cascading.tuple.IndexTuple;
import cascading.tuple.Tuple;
import cascading.tuple.TuplePair;
import cascading.tuple.hadoop.CoGroupingComparator;
import cascading.tuple.hadoop.CoGroupingPartitioner;
import cascading.tuple.hadoop.GroupingComparator;
import cascading.tuple.hadoop.GroupingPartitioner;
import cascading.tuple.hadoop.GroupingSortingComparator;
import cascading.tuple.hadoop.IndexTupleCoGroupingComparator;
import cascading.tuple.hadoop.ReverseGroupingSortingComparator;
import cascading.tuple.hadoop.ReverseTupleComparator;
import cascading.tuple.hadoop.TupleComparator;
import cascading.tuple.hadoop.TupleSerialization;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class HadoopFlowStep extends BaseFlowStep<JobConf>
  {
  /** Field mapperTraps */
  private final Map<String, Tap> mapperTraps = new HashMap<String, Tap>();
  /** Field reducerTraps */
  private final Map<String, Tap> reducerTraps = new HashMap<String, Tap>();

  public HadoopFlowStep( String name, int stepNum )
    {
    super( name, stepNum );
    }

  public JobConf getInitializedConfig( FlowProcess<JobConf> flowProcess, JobConf parentConfig )
    {
    JobConf conf = parentConfig == null ? new JobConf() : new JobConf( parentConfig );

    // disable warning
    conf.setBoolean( "mapred.used.genericoptionsparser", true );

    conf.setJobName( getStepDisplayName() );

    conf.setOutputKeyClass( Tuple.class );
    conf.setOutputValueClass( Tuple.class );

    conf.setMapRunnerClass( FlowMapper.class );
    conf.setReducerClass( FlowReducer.class );

    // set for use by the shuffling phase
    TupleSerialization.setSerializations( conf );

    initFromSources( flowProcess, conf );

    initFromSink( flowProcess, conf );

    initFromTraps( flowProcess, conf );

    initFromPipes( conf );

    if( getSink().getScheme().getNumSinkParts() != 0 )
      {
      // if no reducer, set num map tasks to control parts
      if( getGroup() != null )
        conf.setNumReduceTasks( getSink().getScheme().getNumSinkParts() );
      else
        conf.setNumMapTasks( getSink().getScheme().getNumSinkParts() );
      }

    conf.setOutputKeyComparatorClass( TupleComparator.class );

    if( getGroup() == null )
      {
      conf.setNumReduceTasks( 0 ); // disable reducers
      }
    else
      {
      // must set map output defaults when performing a reduce
      conf.setMapOutputKeyClass( Tuple.class );
      conf.setMapOutputValueClass( Tuple.class );

      // handles the case the groupby sort should be reversed
      if( getGroup().isSortReversed() )
        conf.setOutputKeyComparatorClass( ReverseTupleComparator.class );

      addComparators( conf, "cascading.group.comparator", getGroup().getKeySelectors() );

      if( getGroup().isGroupBy() )
        addComparators( conf, "cascading.sort.comparator", getGroup().getSortingSelectors() );

      if( !getGroup().isGroupBy() )
        {
        conf.setPartitionerClass( CoGroupingPartitioner.class );
        conf.setMapOutputKeyClass( IndexTuple.class ); // allows groups to be sorted by index
        conf.setMapOutputValueClass( IndexTuple.class );
        conf.setOutputKeyComparatorClass( IndexTupleCoGroupingComparator.class ); // sorts by group, then by index
        conf.setOutputValueGroupingComparator( CoGroupingComparator.class );
        }

      if( getGroup().isSorted() )
        {
        conf.setPartitionerClass( GroupingPartitioner.class );
        conf.setMapOutputKeyClass( TuplePair.class );

        if( getGroup().isSortReversed() )
          conf.setOutputKeyComparatorClass( ReverseGroupingSortingComparator.class );
        else
          conf.setOutputKeyComparatorClass( GroupingSortingComparator.class );

        // no need to supply a reverse comparator, only equality is checked
        conf.setOutputValueGroupingComparator( GroupingComparator.class );
        }
      }

    // perform last so init above will pass to tasks
    conf.set( CASCADING_FLOW_STEP_ID, getID() );
    conf.set( "cascading.flow.step.num", Integer.toString( getStepNum() ) );
    conf.set( "cascading.flow.step", pack( this ) );

    return conf;
    }

  private String pack( Object object )
    {
    try
      {
      if( object instanceof Map )
        return HadoopUtil.serializeMapBase64( (Map<String, String>) object, true );

      return HadoopUtil.serializeBase64( object );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to pack object: " + object.getClass().getCanonicalName(), exception );
      }
    }

  protected FlowStepJob<JobConf> createFlowStepJob( FlowProcess<JobConf> flowProcess, JobConf parentConfig )
    {
    JobConf initializedConfig = getInitializedConfig( flowProcess, parentConfig );

    setConf( initializedConfig );

    return new HadoopFlowStepJob( createClientState( flowProcess ), this, initializedConfig );
    }

  /**
   * Method clean removes any temporary files used by this FlowStep instance. It will log any IOExceptions thrown.
   *
   * @param config of type JobConf
   */
  public void clean( JobConf config )
    {
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

    if( getSink() instanceof TempHfs )
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
      cleanTap( config, getSink() );
      }

    for( Tap tap : getMapperTraps().values() )
      cleanTap( config, tap );

    for( Tap tap : getReducerTraps().values() )
      cleanTap( config, tap );

    }

  private void cleanTap( JobConf jobConf, Tap tap )
    {
    try
      {
      Hadoop18TapUtil.cleanupTap( jobConf, tap );
      }
    catch( IOException exception )
      {
      // ignore exception
      }
    }

  private void addComparators( JobConf conf, String property, Map<String, Fields> map )
    {
    Iterator<Fields> fieldsIterator = map.values().iterator();

    if( !fieldsIterator.hasNext() )
      return;

    Fields fields = fieldsIterator.next();

    if( fields.hasComparators() )
      {
      conf.set( property, pack( fields ) );
      return;
      }

    // use resolved fields if there are no comparators.
    Set<Scope> previousScopes = getPreviousScopes( getGroup() );

    fields = previousScopes.iterator().next().getOutValuesFields();

    if( fields.size() != 0 ) // allows fields.UNKNOWN to be used
      conf.setInt( property + ".size", fields.size() );

    return;
    }

  private void initFromTraps( FlowProcess<JobConf> flowProcess, JobConf conf, Map<String, Tap> traps )
    {
    if( !traps.isEmpty() )
      {
      JobConf trapConf = new JobConf( conf );

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
      tap.sourceConfInit( flowProcess, streamedJobs[ i ] );
      streamedJobs[ i ].set( "cascading.step.source", tap.getIdentifier() );
      i++;
      }

    Set<Tap> accumulatedSources = getAllAccumulatedSources();

    for( Tap tap : accumulatedSources )
      {
      JobConf accumulatedJob = flowProcess.copyConfig( conf );
      tap.sourceConfInit( flowProcess, accumulatedJob );
      Map<String, String> map = flowProcess.diffConfigIntoMap( conf, accumulatedJob );
      conf.set( "cascading.step.accumulated.source.conf." + tap.getIdentifier(), pack( map ) );
      }

    MultiInputFormat.addInputFormat( conf, streamedJobs ); //must come last
    }

  private void initFromPipes( final JobConf conf )
    {
    initConfFromPipes( getSetterFor( conf ) );
    }

  private ConfigDef.Setter getSetterFor( final JobConf conf )
    {
    return new ConfigDef.Setter()
    {
    @Override
    public String set( String key, String value )
      {
      String oldValue = get( key );

      conf.set( key, value );

      return oldValue;
      }

    @Override
    public String update( String key, String value )
      {
      String oldValue = get( key );

      if( oldValue == null )
        conf.set( key, value );
      else if( !oldValue.contains( value ) )
        conf.set( key, oldValue + "," + value );

      return oldValue;
      }

    @Override
    public String get( String key )
      {
      String value = conf.get( key );

      if( value == null || value.isEmpty() )
        return null;

      return value;
      }
    };
    }

  /**
   * sources are specific to step, remove all known accumulated sources, if any
   *
   * @return
   */
  private Set<Tap> getUniqueStreamedSources()
    {
    HashSet<Tap> set = new HashSet<Tap>( sources.keySet() );

    set.removeAll( getAllAccumulatedSources() );

    return set;
    }

  protected void initFromSink( FlowProcess<JobConf> flowProcess, JobConf conf )
    {
    // init sink first so tempSink can take precedence
    if( getSink() != null )
      getSink().sinkConfInit( flowProcess, conf );

    if( FileOutputFormat.getOutputPath( conf ) == null )
      tempSink = new TempHfs( "tmp:/" + new Path( getSink().getIdentifier() ).toUri().getPath(), true );

    // tempSink exists because sink is writeDirect
    if( tempSink != null )
      tempSink.sinkConfInit( flowProcess, conf );
    }

  protected void initFromTraps( FlowProcess<JobConf> flowProcess, JobConf conf )
    {
    initFromTraps( flowProcess, conf, getMapperTraps() );
    initFromTraps( flowProcess, conf, getReducerTraps() );
    }

  @Override
  public Set<Tap> getTraps()
    {
    Set<Tap> set = new HashSet<Tap>();

    set.addAll( mapperTraps.values() );
    set.addAll( reducerTraps.values() );

    return Collections.unmodifiableSet( set );
    }

  @Override
  public Tap getTrap( String name )
    {
    Tap trap = getMapperTrap( name );

    if( trap == null )
      trap = getReducerTrap( name );

    return trap;
    }

  public Map<String, Tap> getMapperTraps()
    {
    return mapperTraps;
    }

  public Map<String, Tap> getReducerTraps()
    {
    return reducerTraps;
    }

  public Tap getMapperTrap( String name )
    {
    return getMapperTraps().get( name );
    }

  public Tap getReducerTrap( String name )
    {
    return getReducerTraps().get( name );
    }
  }
