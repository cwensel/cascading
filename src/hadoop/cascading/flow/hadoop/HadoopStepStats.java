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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowException;
import cascading.flow.planner.FlowStep;
import cascading.management.ClientState;
import cascading.stats.FlowStepStats;
import cascading.util.Util;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class HadoopStepStats provides Hadoop specific statistics and methods to underyling Hadoop facilities. */
public abstract class HadoopStepStats extends FlowStepStats
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HadoopStepStats.class );

  private Map<TaskID, String> idCache = new HashMap<TaskID, String>( 4999 ); // nearest prime, caching for ids

  /** Field numMapTasks */
  int numMapTasks;
  /** Field numReducerTasks */
  int numReducerTasks;
  /** Field taskStats */
  Map<String, HadoopTaskStats> taskStats = (Map<String, HadoopTaskStats>) Collections.EMPTY_MAP;

  protected HadoopStepStats( JobConf currentConf, FlowStep flowStep, ClientState clientState )
    {
    super( flowStep, clientState );
    }

  /**
   * Method getTaskStats returns the taskStats of this HadoopStepStats object.
   *
   * @return the taskStats (type ArrayList<HadoopTaskStats>) of this HadoopStepStats object.
   */
  public Map<String, HadoopTaskStats> getTaskStats()
    {
    return taskStats;
    }

  protected void setTaskStats( Map<String, HadoopTaskStats> taskStats )
    {
    this.taskStats = taskStats;
    }

  /**
   * Method getNumMapTasks returns the numMapTasks from the Hadoop job file.
   *
   * @return the numMapTasks (type int) of this HadoopStepStats object.
   */
  @Deprecated
  public int getNumMapTasks()
    {
    return numMapTasks;
    }

  @Deprecated
  void setNumMapTasks( int numMapTasks )
    {
    this.numMapTasks = numMapTasks;
    }

  /**
   * Method getNumReducerTasks returns the numReducerTasks from the Hadoop job file.
   *
   * @return the numReducerTasks (type int) of this HadoopStepStats object.
   */
  @Deprecated
  public int getNumReducerTasks()
    {
    return numReducerTasks;
    }

  @Deprecated
  void setNumReducerTasks( int numReducerTasks )
    {
    this.numReducerTasks = numReducerTasks;
    }

  /**
   * Method getJobID returns the Hadoop running job JobID.
   *
   * @return the jobID (type String) of this HadoopStepStats object.
   */
  public String getJobID()
    {
    if( getRunningJob() == null )
      return null;

    return getRunningJob().getJobID();
    }

  /**
   * Method getJobClient returns the Hadoop {@link JobClient} managing this Hadoop job.
   *
   * @return the jobClient (type JobClient) of this HadoopStepStats object.
   */
  public abstract JobClient getJobClient();

  /**
   * Method getRunningJob returns the Hadoop {@link RunningJob} managing this Hadoop job.
   *
   * @return the runningJob (type RunningJob) of this HadoopStepStats object.
   */
  public abstract RunningJob getRunningJob();

  /**
   * Method getCounterGroups returns all of the Hadoop counter groups.
   *
   * @return the counterGroups (type Collection<String>) of this HadoopStepStats object.
   */
  @Override
  public Collection<String> getCounterGroups()
    {
    try
      {
      RunningJob runningJob = getRunningJob();

      if( runningJob == null )
        return Collections.emptySet();

      Counters counters = runningJob.getCounters();

      if( counters == null )
        return Collections.emptySet();

      return Collections.unmodifiableCollection( counters.getGroupNames() );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get remote counter groups" );
      }
    }

  /**
   * Method getCounterGroupsMatching returns all the Hadoop counter groups that match the give regex pattern.
   *
   * @param regex of String
   * @return Collection<String>
   */
  @Override
  public Collection<String> getCounterGroupsMatching( String regex )
    {
    try
      {
      RunningJob runningJob = getRunningJob();

      if( runningJob == null )
        return Collections.emptySet();

      Counters counters = runningJob.getCounters();

      if( counters == null )
        return Collections.emptySet();

      Set<String> results = new HashSet<String>();

      for( String counter : counters.getGroupNames() )
        {
        if( counter.matches( regex ) )
          results.add( counter );
        }

      return Collections.unmodifiableCollection( results );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get remote counter groups" );
      }
    }

  /**
   * Method getCountersFor returns the Hadoop counters for the given group.
   *
   * @param group of String
   * @return Collection<String>
   */
  @Override
  public Collection<String> getCountersFor( String group )
    {
    try
      {
      RunningJob runningJob = getRunningJob();

      if( runningJob == null )
        return Collections.emptySet();

      Counters counters = runningJob.getCounters();

      if( counters == null )
        return Collections.emptySet();

      Set<String> results = new HashSet<String>();

      for( Counters.Counter counter : counters.getGroup( group ) )
        results.add( counter.getName() );

      return Collections.unmodifiableCollection( results );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get remote counter groups" );
      }
    }

  /**
   * Method getCounterValue returns the Hadoop counter value for the given counter enum.
   *
   * @param counter of Enum
   * @return long
   */
  @Override
  public long getCounterValue( Enum counter )
    {
    try
      {
      RunningJob runningJob = getRunningJob();

      if( runningJob == null )
        return 0;

      Counters counters = runningJob.getCounters();

      if( counters == null )
        return 0;

      return counters.getCounter( counter );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get remote counter values" );
      }
    }

  /**
   * Method getCounterValue returns the Hadoop counter value for the given group and counter name.
   *
   * @param group   of String
   * @param counter of String
   * @return long
   */
  @Override
  public long getCounterValue( String group, String counter )
    {
    try
      {
      RunningJob runningJob = getRunningJob();

      if( runningJob == null )
        return 0;

      Counters counters = runningJob.getCounters();

      if( counters == null )
        return 0;

      Counters.Group counterGroup = counters.getGroup( group );

      if( group == null )
        return 0;

      // geCounter actually searches the display name, wtf
      // in theory this is lazily created if does not exist, but don't rely on it
      Counters.Counter counterValue = counterGroup.getCounterForName( counter );

      if( counter == null )
        return 0;

      return counterValue.getValue();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get remote counter values" );
      }
    }

  /**
   * Returns the underlying Map tasks progress percentage.
   * <p/>
   * This method is experimental.
   *
   * @return float
   */
  public float getMapProgress()
    {
    RunningJob runningJob = getRunningJob();

    if( runningJob == null )
      return 0;

    try
      {
      return runningJob.mapProgress();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get progress" );
      }
    }

  /**
   * Returns the underlying Reduce tasks progress percentage.
   * <p/>
   * This method is experimental.
   *
   * @return float
   */
  public float getReduceProgress()
    {
    RunningJob runningJob = getRunningJob();

    if( runningJob == null )
      return 0;

    try
      {
      return runningJob.reduceProgress();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get progress" );
      }
    }

  public String getStatusURL()
    {
    RunningJob runningJob = getRunningJob();

    if( runningJob == null )
      return null;

    return runningJob.getTrackingURL();
    }

  public void captureJobStats()
    {
    // do nothing
    }

  /**
   * Method getChildren returns the children of this HadoopStepStats object.
   *
   * @return the children (type Collection) of this HadoopStepStats object.
   */
  @Override
  public Collection getChildren()
    {
    return getTaskStats().values();
    }

  public Set<String> getChildIDs()
    {
    return getTaskStats().keySet();
    }

  /** Synchronized to prevent state changes mid record, #stop may be called out of band */
  @Override
  public synchronized void recordChildStats()
    {
    // if null instance don't bother capturing detail
    if( !clientState.isEnabled() )
      return;

    captureDetail();

    try
      {
      for( String id : taskStats.keySet() )
        clientState.record( id, taskStats.get( id ) );
      }
    catch( Exception exception )
      {
      LOG.error( "unable to record slice stats", exception );
      }
    }

  /** Method captureDetail captures statistics task details and completion events. */
  @Override
  public synchronized void captureDetail()
    {
    captureJobStats();

    captureDetail( true );
    }

  public void captureDetail( boolean captureAttempts )
    {
    HashMap<String, HadoopTaskStats> newStats = new HashMap<String, HadoopTaskStats>();

    JobClient jobClient = getJobClient();
    RunningJob runningJob = getRunningJob();

    if( jobClient == null || runningJob == null )
      return;

    try
      {
      // cleanup/setup tasks have no useful info so far.
//      addTaskStats( newStats, HadoopTaskStats.Kind.SETUP, jobClient.getSetupTaskReports( runningJob.getID() ), false );
//      addTaskStats( newStats, HadoopTaskStats.Kind.CLEANUP, jobClient.getCleanupTaskReports( runningJob.getID() ), false );
      addTaskStats( newStats, HadoopTaskStats.Kind.MAPPER, jobClient.getMapTaskReports( runningJob.getID() ), false );
      addTaskStats( newStats, HadoopTaskStats.Kind.REDUCER, jobClient.getReduceTaskReports( runningJob.getID() ), false );

      int count = 0;

      while( captureAttempts )
        {
        TaskCompletionEvent[] events = runningJob.getTaskCompletionEvents( count );

        if( events.length == 0 )
          break;

        addAttemptsToTaskStats( newStats, events );
        count += 10;
        }

      setTaskStats( newStats );
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to get task stats", exception );
      }
    }

  private void addTaskStats( Map<String, HadoopTaskStats> taskStats, HadoopTaskStats.Kind kind, TaskReport[] taskReports, boolean skipLast )
    {
    for( int i = 0; i < taskReports.length - ( skipLast ? 1 : 0 ); i++ )
      {
      TaskReport taskReport = taskReports[ i ];

      if( taskReport == null )
        {
        LOG.warn( "found empty task report" );
        continue;
        }

      String id = getIDFor( taskReport.getTaskID() );
      taskStats.put( id, new HadoopTaskStats( id, getStatus(), kind, taskReport ) );
      }
    }

  private void addAttemptsToTaskStats( Map<String, HadoopTaskStats> taskStats, TaskCompletionEvent[] events )
    {
    for( TaskCompletionEvent event : events )
      {
      if( event == null )
        {
        LOG.warn( "found empty completion event" );
        continue;
        }

      // this will return a housekeeping task, which we are not tracking
      HadoopTaskStats stats = taskStats.get( getIDFor( event.getTaskAttemptId().getTaskID() ) );

      if( stats != null )
        stats.addAttempt( event );
      }
    }

  private String getIDFor( TaskID taskID )
    {
    // using taskID instance as #toString is quite painful
    String id = idCache.get( taskID );

    if( id == null )
      {
      id = Util.createID( taskID.toString() );
      idCache.put( taskID, id );
      }

    return id;
    }
  }
