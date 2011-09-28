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

/** Class HadoopStepStats ... */
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

  /** Class HadoopTaskStats ... */
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

    public HadoopTaskStats( TaskType taskType, TaskReport taskReport )
      {
      fill( taskType, taskReport );
      }

    public HadoopTaskStats( TaskCompletionEvent taskCompletionEvent )
      {
      fill( taskCompletionEvent );
      }

    public String getId()
      {
      return id;
      }

    public void fill( TaskCompletionEvent taskCompletionEvent )
      {
      taskType = taskCompletionEvent.getTaskAttemptId().getTaskID().isMap() ? TaskType.MAPPER : TaskType.REDUCER;
      status = taskCompletionEvent.getTaskStatus().toString();
      }

    public void fill( TaskType taskType, TaskReport taskReport )
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

    public long getCounterValue( Enum counter )
      {
      return getCounterValue( counter.getDeclaringClass().getName(), counter.name() );
      }

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

  public int getNumMapTasks()
    {
    return numMapTasks;
    }

  public void setNumMapTasks( int numMapTasks )
    {
    this.numMapTasks = numMapTasks;
    }

  public int getNumReducerTasks()
    {
    return numReducerTasks;
    }

  public void setNumReducerTasks( int numReducerTasks )
    {
    this.numReducerTasks = numReducerTasks;
    }

  public String getJobID()
    {
    return getRunningJob().getJobID();
    }

  protected abstract JobClient getJobClient();

  protected abstract RunningJob getRunningJob();

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
   * Returns the underlying Map tasks progress.
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
   * Returns the underlying Reduce tasks progress.
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

  public void captureJobStats()
    {
    RunningJob runningJob = getRunningJob();

    if( runningJob == null )
      return;

    JobConf ranJob = new JobConf( runningJob.getJobFile() );

    setNumMapTasks( ranJob.getNumMapTasks() );
    setNumReducerTasks( ranJob.getNumReduceTasks() );
    }

  @Override
  public Collection getChildren()
    {
    return getTaskStats();
    }

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
