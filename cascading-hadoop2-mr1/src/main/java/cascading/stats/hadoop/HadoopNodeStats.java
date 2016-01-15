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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowNode;
import cascading.management.state.ClientState;
import cascading.stats.BaseCachedNodeStats;
import cascading.stats.CounterCache;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowSliceStats;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;

import static cascading.util.Util.formatDurationFromMillis;

/**
 *
 */
public class HadoopNodeStats extends BaseCachedNodeStats<Configuration, FlowNodeStats, Map<String, Map<String, Long>>>
  {
  private Map<TaskID, String> sliceIDCache = new HashMap<TaskID, String>( 4999 ); // caching for ids

  private HadoopStepStats parentStepStats;
  private HadoopSliceStats.Kind kind;

  /**
   * Constructor CascadingStats creates a new CascadingStats instance.
   *
   * @param parentStepStats
   * @param configuration
   * @param kind
   * @param flowNode
   * @param clientState
   */
  protected HadoopNodeStats( final HadoopStepStats parentStepStats, Configuration configuration, HadoopSliceStats.Kind kind, FlowNode flowNode, ClientState clientState )
    {
    super( flowNode, clientState );
    this.parentStepStats = parentStepStats;
    this.kind = kind;

    this.counterCache = new HadoopNodeCounterCache( this, configuration );
    }

  @Override
  public String getKind()
    {
    if( kind == null )
      return null;

    return kind.name();
    }

  private Status getParentStatus()
    {
    return parentStepStats.getStatus();
    }

  private RunningJob getJobStatusClient()
    {
    return parentStepStats.getJobStatusClient();
    }

  /**
   * Retrieves the TaskReports via the mapreduce API.
   *
   * @param kind The kind of TaskReport to retrieve.
   * @return An array of TaskReports, but never <code>nul</code>.
   * @throws IOException
   */
  private TaskReport[] retrieveTaskReports( HadoopSliceStats.Kind kind ) throws IOException, InterruptedException
    {
    Job job = HadoopStepStats.getJob( getJobStatusClient() );

    if( job == null )
      return new TaskReport[ 0 ];

    switch( kind )
      {
      case MAPPER:
        return job.getTaskReports( TaskType.MAP );
      case REDUCER:
        return job.getTaskReports( TaskType.REDUCE );
      case SETUP:
        return job.getTaskReports( TaskType.JOB_SETUP );
      case CLEANUP:
        return job.getTaskReports( TaskType.JOB_CLEANUP );
      default:
        return new TaskReport[ 0 ];
      }
    }

  @Override
  protected boolean captureChildDetailInternal()
    {
    if( allChildrenFinished )
      return true;

    Job job = HadoopStepStats.getJob( getJobStatusClient() );

    if( job == null )
      return false;

    try
      {
      TaskReport[] taskReports = retrieveTaskReports( kind );

      if( taskReports.length == 0 )
        return false;

      addTaskStats( taskReports, false );

      return true;
      }
    catch( IOException exception )
      {
      logWarn( "unable to retrieve slice stats via task reports", exception );
      }
    catch( InterruptedException exception )
      {
      logWarn( "retrieving task reports timed out, consider increasing timeout delay in CounterCache via: '{}', message: {}", CounterCache.COUNTER_TIMEOUT_PROPERTY, exception.getMessage() );
      }

    return false;
    }

  protected void addTaskStats( TaskReport[] taskReports, boolean skipLast )
    {
    logInfo( "retrieved task reports: {}", taskReports.length );

    long lastFetch = System.currentTimeMillis();
    boolean fetchedAreFinished = true;

    synchronized( sliceStatsMap )
      {
      int added = 0;
      int updated = 0;

      for( int i = 0; i < taskReports.length - ( skipLast ? 1 : 0 ); i++ )
        {
        TaskReport taskReport = taskReports[ i ];

        if( taskReport == null )
          {
          logWarn( "found empty task report" );
          continue;
          }

        String id = getSliceIDFor( taskReport.getTaskID() );
        HadoopSliceStats sliceStats = (HadoopSliceStats) sliceStatsMap.get( id );

        if( sliceStats != null )
          {
          sliceStats.update( getParentStatus(), kind, taskReport, lastFetch );
          updated++;
          }
        else
          {
          sliceStats = new HadoopSliceStats( id, getParentStatus(), kind, taskReport, lastFetch );
          sliceStatsMap.put( id, sliceStats );
          added++;
          }

        if( !sliceStats.getStatus().isFinished() )
          fetchedAreFinished = false;
        }

      int total = sliceStatsMap.size();
      String duration = formatDurationFromMillis( System.currentTimeMillis() - lastFetch );

      logInfo( "added {}, updated: {} slices, with duration: {}, total fetched: {}", added, updated, duration, total );
      }

    allChildrenFinished = taskReports.length != 0 && fetchedAreFinished;
    }

  protected void addAttempt( TaskCompletionEvent event )
    {
    // the event could be a housekeeping task, which we are not tracking
    String sliceID = sliceIDCache.get( event.getTaskAttemptId().getTaskID() );

    if( sliceID == null )
      return;

    FlowSliceStats stats;

    synchronized( sliceStatsMap )
      {
      stats = sliceStatsMap.get( sliceID );
      }

    if( stats == null )
      return;

    ( (HadoopSliceStats) stats ).addAttempt( event );
    }

  private String getSliceIDFor( TaskID taskID )
    {
    // using taskID instance as #toString is quite painful
    String id = sliceIDCache.get( taskID );

    if( id == null )
      {
      id = Util.createUniqueID();
      sliceIDCache.put( taskID, id );
      }

    return id;
    }
  }
