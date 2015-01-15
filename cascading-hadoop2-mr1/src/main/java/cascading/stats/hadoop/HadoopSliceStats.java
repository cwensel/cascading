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

package cascading.stats.hadoop;

import java.util.HashMap;
import java.util.Map;

import cascading.stats.CascadingStats;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;

import static cascading.stats.CascadingStats.Status.*;

/** Class HadoopTaskStats tracks individual task stats. */
public class HadoopSliceStats
  {
  private final CascadingStats.Status parentStatus;

  public static class HadoopAttempt
    {
    private final TaskCompletionEvent event;

    public HadoopAttempt( TaskCompletionEvent event )
      {
      this.event = event;
      }

    public int getEventId()
      {
      return event.getEventId();
      }

    public int getTaskRunTime()
      {
      return event.getTaskRunTime();
      }

    public String getTaskStatus()
      {
      return event.getStatus().toString();
      }

    public String getTaskTrackerHttp()
      {
      return event.getTaskTrackerHttp();
      }

    public CascadingStats.Status getStatusFor()
      {
      CascadingStats.Status status = null;

      switch( event.getStatus() )
        {
        case FAILED:
          status = FAILED;
          break;
        case KILLED:
          status = STOPPED;
          break;
        case SUCCEEDED:
          status = SUCCESSFUL;
          break;
        case OBSOLETE:
          status = SKIPPED;
          break;
        case TIPFAILED:
          status = FAILED;
          break;
        }
      return status;
      }
    }

  public enum Kind
    {
      SETUP, MAPPER, REDUCER, CLEANUP
    }

  private String id;
  private Kind kind;
  private final boolean parentStepHasReducers;
  private TaskReport taskReport;
  private Map<String, Map<String, Long>> counters;

  private Map<Integer, HadoopAttempt> attempts = new HashMap<Integer, HadoopAttempt>();

  HadoopSliceStats( String id, CascadingStats.Status parentStatus, Kind kind, boolean parentStepHasReducers, TaskReport taskReport )
    {
    this.parentStatus = parentStatus;
    this.id = id;
    this.kind = kind;
    this.parentStepHasReducers = parentStepHasReducers;
    this.taskReport = taskReport;
    }

  public String getID()
    {
    return id;
    }

  public Kind getKind()
    {
    return kind;
    }

  /**
   * Method getId returns the Hadoop task id.
   *
   * @return the id (type String) of this HadoopTaskStats object.
   */
  public String getTaskID()
    {
    return taskReport.getTaskID().toString();
    }

  public int getTaskIDNum()
    {
    return taskReport.getTaskID().getId();
    }

  public String getJobID()
    {
    return taskReport.getTaskID().getJobID().toString();
    }

  public boolean parentStepHasReducers()
    {
    return parentStepHasReducers;
    }

  public float getProgress()
    {
    return taskReport.getProgress();
    }

  public String getState()
    {
    return taskReport.getState();
    }

  public long getStartTime()
    {
    return taskReport.getStartTime();
    }

  public long getFinishTime()
    {
    return taskReport.getFinishTime();
    }

  public CascadingStats.Status getParentStatus()
    {
    return parentStatus;
    }

  public CascadingStats.Status getStatus()
    {
    CascadingStats.Status status = null;

    switch( taskReport.getCurrentStatus() )
      {
      case PENDING:
        status = PENDING;
        break;
      case RUNNING:
        status = RUNNING;
        break;
      case COMPLETE:
        status = SUCCESSFUL;
        break;
      case KILLED:
        status = STOPPED;
        break;
      case FAILED:
        status = FAILED;
        break;
      }

    return status;
    }

  public String[] getDiagnostics()
    {
    return taskReport.getDiagnostics();
    }

  public Map<String, Map<String, Long>> getCounters()
    {
    if( counters == null )
      setCounters( taskReport );

    return counters;
    }

  public Map<Integer, HadoopAttempt> getAttempts()
    {
    return attempts;
    }

  private void setCounters( TaskReport taskReport )
    {
    this.counters = new HashMap<String, Map<String, Long>>();

    Counters hadoopCounters = taskReport.getTaskCounters();

    for( CounterGroup group : hadoopCounters )
      {
      Map<String, Long> values = new HashMap<String, Long>();
      this.counters.put( group.getName(), values );

      for( Counter counter : group )
        values.put( counter.getName(), counter.getValue() );
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
    if( getCounters() == null || getCounters().get( group ) == null )
      return 0;

    Long value = getCounters().get( group ).get( name );

    if( value == null )
      return 0;

    return value;
    }

  public void addAttempt( TaskCompletionEvent event )
    {
    attempts.put( event.getEventId(), new HadoopAttempt( event ) );
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "HadoopTaskStats" );
    sb.append( "{id='" ).append( id ).append( '\'' );
    sb.append( ", kind=" ).append( kind );
    sb.append( '}' );
    return sb.toString();
    }
  }
