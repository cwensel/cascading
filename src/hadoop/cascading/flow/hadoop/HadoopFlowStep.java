/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.flow.planner.FlowStep;
import cascading.flow.planner.FlowStepJob;
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
public class HadoopFlowStep extends FlowStep<JobConf>
  {
  /** Field mapperTraps */
  private final Map<String, Tap> mapperTraps = new HashMap<String, Tap>();
  /** Field reducerTraps */
  private final Map<String, Tap> reducerTraps = new HashMap<String, Tap>();

  public HadoopFlowStep( String name, int id )
    {
    super( name, id );
    }

  public JobConf getInitializedConfig( FlowProcess<JobConf> flowProcess, JobConf parentConfig ) throws IOException
    {
    JobConf conf = parentConfig == null ? new JobConf() : new JobConf( parentConfig );

    // set values first so they can't break things downstream
    if( hasProperties() )
      {
      for( Map.Entry entry : getProperties().entrySet() )
        conf.set( entry.getKey().toString(), entry.getValue().toString() );
      }

    // disable warning
    conf.setBoolean( "mapred.used.genericoptionsparser", true );

    conf.setJobName( getStepName() );

    conf.setOutputKeyClass( Tuple.class );
    conf.setOutputValueClass( Tuple.class );

    conf.setMapRunnerClass( FlowMapper.class );
    conf.setReducerClass( FlowReducer.class );

    // set for use by the shuffling phase
    TupleSerialization.setSerializations( conf );

    initFromSources( flowProcess, conf );

    initFromSink( flowProcess, conf );

    initFromTraps( flowProcess, conf );

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

      addComparators( conf, "cascading.group.comparator", getGroup().getGroupingSelectors() );

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
    conf.setInt( "cascading.flow.step.id", getID() );
    conf.set( "cascading.flow.step", HadoopUtil.serializeBase64( this ) );

    return conf;
    }

  public FlowStepJob createFlowStepJob( FlowProcess<JobConf> flowProcess, JobConf parentConfig ) throws IOException
    {
    return new HadoopFlowStepJob( this, getName(), getInitializedConfig( flowProcess, parentConfig ) );
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
        tempSink.deletePath( config );
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
        getSink().deletePath( config );
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

  private void addComparators( JobConf conf, String property, Map<String, Fields> map ) throws IOException
    {
    Iterator<Fields> fieldsIterator = map.values().iterator();

    if( !fieldsIterator.hasNext() )
      return;

    Fields fields = fieldsIterator.next();

    if( fields.hasComparators() )
      {
      conf.set( property, HadoopUtil.serializeBase64( fields ) );
      return;
      }

    // use resolved fields if there are no comparators.
    Set<Scope> previousScopes = getPreviousScopes( getGroup() );

    fields = previousScopes.iterator().next().getOutValuesFields();

    if( fields.size() != 0 ) // allows fields.UNKNOWN to be used
      conf.setInt( property + ".size", fields.size() );

    return;
    }

  private void initFromTraps( FlowProcess<JobConf> flowProcess, JobConf conf, Map<String, Tap> traps ) throws IOException
    {
    if( !traps.isEmpty() )
      {
      JobConf trapConf = new JobConf( conf );

      for( Tap tap : traps.values() )
        tap.sinkConfInit( flowProcess, trapConf );
      }
    }

  protected void initFromSources( FlowProcess<JobConf> flowProcess, JobConf conf ) throws IOException
    {
    // handles case where same tap is used on multiple branches
    // we do not want to init the same tap multiple times
    Set<Tap> uniqueSources = new HashSet<Tap>( sources.values() );

    JobConf[] fromJobs = new JobConf[ uniqueSources.size() ];
    int i = 0;

    for( Tap tap : uniqueSources )
      {
      fromJobs[ i ] = flowProcess.copyConfig( conf );
      tap.sourceConfInit( flowProcess, fromJobs[ i ] );
      fromJobs[ i ].set( "cascading.step.source", HadoopUtil.serializeBase64( tap ) );
      i++;
      }

    MultiInputFormat.addInputFormat( conf, fromJobs );
    }

  protected void initFromSink( FlowProcess<JobConf> flowProcess, JobConf conf ) throws IOException
    {
    // init sink first so tempSink can take precedence
    if( getSink() != null )
      getSink().sinkConfInit( flowProcess, conf );

    if( FileOutputFormat.getOutputPath( conf ) == null )
      tempSink = new TempHfs( "tmp:/" + new Path( getSink().getPath() ).toUri().getPath(), true );

    // tempSink exists because sink is writeDirect
    if( tempSink != null )
      tempSink.sinkConfInit( flowProcess, conf );
    }

  protected void initFromTraps( FlowProcess<JobConf> flowProcess, JobConf conf ) throws IOException
    {
    initFromTraps( flowProcess, conf, getMapperTraps() );
    initFromTraps( flowProcess, conf, getReducerTraps() );
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
