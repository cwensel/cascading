/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowException;
import cascading.stats.StepStats;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.log4j.Logger;

/** Class HadoopStepStats ... */
public abstract class HadoopStepStats extends StepStats
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( HadoopStepStats.class );

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
      org.apache.hadoop.mapreduce.TaskType type = taskCompletionEvent.getTaskAttemptId().getTaskID().getTaskType();
      taskType = type == org.apache.hadoop.mapreduce.TaskType.MAP ? TaskType.MAPPER : TaskType.REDUCER;
      status = taskCompletionEvent.getStatus().toString();
      }

    public void fill( TaskType taskType, TaskReport taskReport )
      {
      this.taskType = taskType;
      this.id = taskReport.getTaskId().toString();
      this.startTime = taskReport.getStartTime();
      this.finishTime = taskReport.getFinishTime();
      this.state = taskReport.getState();
      this.status = TaskCompletionEvent.Status.SUCCEEDED.toString();

      setCounters( taskReport );
      }

    private void setCounters( TaskReport taskReport )
      {
      this.counters = new HashMap<String, Long>();

      Counters hadoopCounters = taskReport.getTaskCounters();

      for( CounterGroup group : hadoopCounters )
        {
        for( Counter counter : group )
          this.counters.put( group.getName() + "." + counter.getName(), counter.getValue() );
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
      if( event.getStatus() != TaskCompletionEvent.Status.SUCCEEDED )
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

  protected abstract JobStatus getRunningJob();

  protected abstract Job getJob();

  @Override
  public long getCounterValue( Enum counter )
    {
    try
      {
      return getJob().getCounters().findCounter( counter ).getValue();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get counter values", exception );
      }
    catch( InterruptedException exception )
      {
      throw new FlowException( "unable to get counter values", exception );
      }
    }

  public void captureJobStats()
    {
    JobStatus runningJob = getRunningJob();
    JobConf ranJob = new JobConf( runningJob.getJobFile() );

    setNumMapTasks( ranJob.getNumMapTasks() );
    setNumReducerTasks( ranJob.getNumReduceTasks() );
    }

  public Collection getChildren()
    {
    return getTaskStats();
    }

  @Override
  public void captureDetail()
    {
    getTaskStats().clear();

    Job currentJob = getJob();

    try
      {
      addTaskStats( HadoopTaskStats.TaskType.SETUP, currentJob.getTaskReports( TaskType.JOB_SETUP ), true );
      addTaskStats( HadoopTaskStats.TaskType.MAPPER, currentJob.getTaskReports( TaskType.MAP ), false );
      addTaskStats( HadoopTaskStats.TaskType.REDUCER, currentJob.getTaskReports( TaskType.REDUCE ), false );
      addTaskStats( HadoopTaskStats.TaskType.CLEANUP, currentJob.getTaskReports( TaskType.JOB_CLEANUP ), true );

      addTaskStats( currentJob.getTaskCompletionEvents( 0, Integer.MAX_VALUE ) );
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to get task stats", exception );
      }
    catch( InterruptedException exception )
      {
      LOG.warn( "unable to get task stats", exception );
      }
    }

  }
