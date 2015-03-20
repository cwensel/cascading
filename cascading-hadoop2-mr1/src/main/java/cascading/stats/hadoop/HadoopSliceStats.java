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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import cascading.stats.CascadingStats;
import cascading.stats.FlowSliceStats;
import cascading.stats.ProvidesCounters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;

import static cascading.stats.CascadingStats.Status.*;

/** Class HadoopTaskStats tracks individual task stats. */
public class HadoopSliceStats extends FlowSliceStats<HadoopSliceStats.Kind> implements ProvidesCounters
  {
  private final CascadingStats.Status parentStatus;

  public static class HadoopAttempt extends FlowSliceAttempt
    {
    private final TaskCompletionEvent event;

    public HadoopAttempt( TaskCompletionEvent event )
      {
      this.event = event;
      }

    @Override
    public String getProcessAttemptID()
      {
      return event.getTaskAttemptId().toString();
      }

    @Override
    public int getEventId()
      {
      return event.getEventId();
      }

    @Override
    public int getProcessDuration()
      {
      return event.getTaskRunTime();
      }

    @Override
    public String getProcessStatus()
      {
      return event.getStatus().toString();
      }

    @Override
    public String getStatusURL()
      {
      return event.getTaskTrackerHttp();
      }

    @Override
    public CascadingStats.Status getStatus()
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
  private TaskReport taskReport;
  private Map<String, Map<String, Long>> counters;

  private Map<Integer, FlowSliceAttempt> attempts = new HashMap<>();

  HadoopSliceStats( String id, CascadingStats.Status parentStatus, Kind kind, TaskReport taskReport )
    {
    this.parentStatus = parentStatus;
    this.id = id;
    this.kind = kind;
    this.taskReport = taskReport;
    }

  @Override
  public String getID()
    {
    return id;
    }

  @Override
  public Kind getKind()
    {
    return kind;
    }

  @Override
  public String getProcessSliceID()
    {
    return taskReport.getTaskID().toString();
    }

  public int getTaskIDNum()
    {
    return taskReport.getTaskID().getId();
    }

  @Override
  public String getProcessStepID()
    {
    return taskReport.getTaskID().getJobID().toString();
    }

  protected TaskReport getTaskReport()
    {
    return taskReport;
    }

  public float getProgress()
    {
    return taskReport.getProgress();
    }

  @Override
  public String getProcessStatus()
    {
    return taskReport.getState();
    }

  @Override
  public float getProcessProgress()
    {
    return taskReport.getProgress();
    }

  @Override
  public long getProcessStartTime()
    {
    return taskReport.getStartTime();
    }

  @Override
  public long getProcessFinishTime()
    {
    return taskReport.getFinishTime();
    }

  public CascadingStats.Status getParentStatus()
    {
    return parentStatus;
    }

  @Override
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

  @Override
  public String[] getDiagnostics()
    {
    return taskReport.getDiagnostics();
    }

  @Override
  public Map<String, Map<String, Long>> getCounters()
    {
    if( counters == null )
      setCounters( taskReport );

    return counters;
    }

  @Override
  public Map<Integer, FlowSliceAttempt> getAttempts()
    {
    return attempts;
    }

  private void setCounters( TaskReport taskReport )
    {
    this.counters = new HashMap<>();

    Counters hadoopCounters = taskReport.getTaskCounters();

    for( CounterGroup group : hadoopCounters )
      {
      Map<String, Long> values = new HashMap<String, Long>();

      this.counters.put( group.getName(), values );

      for( Counter counter : group )
        values.put( counter.getName(), counter.getValue() );
      }
    }

  @Override
  public Collection<String> getCounterGroups()
    {
    return getCounters().keySet();
    }

  @Override
  public Collection<String> getCountersFor( String group )
    {
    return getCounters().get( group ).keySet();
    }

  @Override
  public Collection<String> getCountersFor( Class<? extends Enum> group )
    {
    return getCountersFor( group.getDeclaringClass().getName() );
    }

  @Override
  public long getCounterValue( Enum counter )
    {
    return getCounterValue( counter.getDeclaringClass().getName(), counter.name() );
    }

  @Override
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
    sb.append( "HadoopSliceStats" );
    sb.append( "{id='" ).append( id ).append( '\'' );
    sb.append( ", kind=" ).append( kind );
    sb.append( '}' );
    return sb.toString();
    }
  }
