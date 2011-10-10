/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowException;
import cascading.flow.planner.FlowStep;
import cascading.management.ClientState;
import cascading.stats.StepStats;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class HadoopStepStats provides Hadoop specific statistics and methods to underyling Hadoop facilities. */
public abstract class HadoopStepStats extends StepStats
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HadoopStepStats.class );

  /** Field numMapTasks */
  int numMapTasks;
  /** Field numReducerTasks */
  int numReducerTasks;
  /** Field taskStats */
  ArrayList<HadoopTaskStats> taskStats;

  /** Class HadoopTaskStats tracks individual task stats. */
  public static class HadoopTaskStats
    {
    public enum TaskType
      {
        SETUP, MAPPER, REDUCER, CLEANUP
      }

    /** Field taskType */
    public TaskType taskType;
    /** Field id */
    public String id;
    /** Field startTime */
    public long startTime;
    /** Field finishTime */
    public long finishTime;
    /** Field status */
    public String status;
    /** Field state */
    public String state;
    /** Field counters */
    public Map<String, Long> counters;

    HadoopTaskStats( TaskType taskType, TaskReport taskReport )
      {
      fill( taskType, taskReport );
      }

    HadoopTaskStats( TaskCompletionEvent taskCompletionEvent )
      {
      fill( taskCompletionEvent );
      }

    /**
     * Method getId returns the Hadoop task id.
     *
     * @return the id (type String) of this HadoopTaskStats object.
     */
    public String getId()
      {
      return id;
      }

    void fill( TaskCompletionEvent taskCompletionEvent )
      {
      taskType = taskCompletionEvent.getTaskAttemptId().getTaskID().isMap() ? TaskType.MAPPER : TaskType.REDUCER;
      status = taskCompletionEvent.getTaskStatus().toString();
      }

    void fill( TaskType taskType, TaskReport taskReport )
      {
      this.taskType = taskType;
      this.id = taskReport.getTaskID().toString();
      this.startTime = taskReport.getStartTime();
      this.finishTime = taskReport.getFinishTime();
      this.state = taskReport.getState();
      this.status = TaskCompletionEvent.Status.SUCCEEDED.toString();

      setCounters( taskReport );
      }

    private void setCounters( TaskReport taskReport )
      {
      this.counters = new HashMap<String, Long>();

      Counters hadoopCounters = taskReport.getCounters();

      for( Counters.Group group : hadoopCounters )
        {
        for( Counters.Counter counter : group )
          this.counters.put( group.getName() + "." + counter.getName(), counter.getCounter() );
        }
      }

    /**
     * Method getCounterValue returns the raw Hadoop counter value.
     *
     * @param counter of Enum
     * @return long
     */
    public long getCounterValue( Enum counter )
      {
      return getCounterValue( counter.getDeclaringClass().getName(), counter.name() );
      }

    /**
     * Method getCounterValue returns the raw Hadoop counter value.
     *
     * @param group of String
     * @param name  of String
     * @return long
     */
    public long getCounterValue( String group, String name )
      {
      if( counters == null )
        return 0;

      Long value = counters.get( group + "." + name );

      if( value == null )
        return 0;

      return value;
      }
    }

  protected HadoopStepStats( JobConf currentConf, FlowStep flowStep, ClientState clientState )
    {
    super( flowStep, clientState );
    }

  /**
   * Method getTaskStats returns the taskStats of this HadoopStepStats object.
   *
   * @return the taskStats (type ArrayList<HadoopTaskStats>) of this HadoopStepStats object.
   */
  public ArrayList<HadoopTaskStats> getTaskStats()
    {
    if( taskStats == null )
      taskStats = new ArrayList<HadoopTaskStats>();

    return taskStats;
    }

  private void addTaskStats( HadoopTaskStats.TaskType taskType, TaskReport[] taskReports, boolean skipLast )
    {
    for( int i = 0; i < taskReports.length - ( skipLast ? 1 : 0 ); i++ )
      getTaskStats().add( new HadoopTaskStats( taskType, taskReports[ i ] ) );
    }

  private void addTaskStats( TaskCompletionEvent[] events )
    {
    for( TaskCompletionEvent event : events )
      {
      if( event.getTaskStatus() != TaskCompletionEvent.Status.SUCCEEDED )
        getTaskStats().add( new HadoopTaskStats( event ) );
      }
    }

  /**
   * Method getNumMapTasks returns the numMapTasks from the Hadoop job file.
   *
   * @return the numMapTasks (type int) of this HadoopStepStats object.
   */
  public int getNumMapTasks()
    {
    return numMapTasks;
    }

  void setNumMapTasks( int numMapTasks )
    {
    this.numMapTasks = numMapTasks;
    }

  /**
   * Method getNumReducerTasks returns the numReducerTasks from the Hadoop job file.
   *
   * @return the numReducerTasks (type int) of this HadoopStepStats object.
   */
  public int getNumReducerTasks()
    {
    return numReducerTasks;
    }

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

      return counterGroup.getCounter( counter );
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

  /** Method captureJobStats forces capture of all Hadoop statistics. */
  public void captureJobStats()
    {
    RunningJob runningJob = getRunningJob();

    if( runningJob == null )
      return;

    JobConf ranJob = new JobConf( runningJob.getJobFile() );

    setNumMapTasks( ranJob.getNumMapTasks() );
    setNumReducerTasks( ranJob.getNumReduceTasks() );
    }

  /**
   * Method getChildren returns the children of this HadoopStepStats object.
   *
   * @return the children (type Collection) of this HadoopStepStats object.
   */
  @Override
  public Collection getChildren()
    {
    return getTaskStats();
    }

  /** Method captureDetail captures statistics task details and completion events. */
  @Override
  public void captureDetail()
    {
    getTaskStats().clear();

    JobClient jobClient = getJobClient();

    try
      {
      addTaskStats( HadoopTaskStats.TaskType.SETUP, jobClient.getSetupTaskReports( getRunningJob().getID() ), true );
      addTaskStats( HadoopTaskStats.TaskType.MAPPER, jobClient.getMapTaskReports( getRunningJob().getID() ), false );
      addTaskStats( HadoopTaskStats.TaskType.REDUCER, jobClient.getReduceTaskReports( getRunningJob().getID() ), false );
      addTaskStats( HadoopTaskStats.TaskType.CLEANUP, jobClient.getCleanupTaskReports( getRunningJob().getID() ), true );

      int count = 0;

      while( true )
        {
        TaskCompletionEvent[] events = getRunningJob().getTaskCompletionEvents( count );

        if( events.length == 0 )
          break;

        addTaskStats( events );
        count += 10;
        }
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to get task stats", exception );
      }
    }
  }
